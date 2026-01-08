import { Client, types } from "pg";
import * as fs from "fs";
import { configDotenv } from "dotenv";
import { Liquibase, POSTGRESQL_DEFAULT_CONFIG } from "liquibase";
import { error, log } from "console";

/* ────────────── SETUP ────────────── */
types.setTypeParser(1082, (v: string) => v); // DATE
const output = process.env.GITHUB_OUTPUT;
configDotenv();

/* ────────────── TYPES ────────────── */
type Row = Record<string, any>;
type TablePK = string[];
type FK = { childTable: string; parentTable: string };

type TableDiff = {
    table: string;
    inserts: string[];
    updates: string[];
    deletes: string[];
};

/* ────────────── CONFIG ────────────── */
const refDbConfig = {
    host: process.env.DB_HOST,
    port: Number(process.env.PORT || 5432),
    database: process.env.DB_REFERENCE,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    ssl: { rejectUnauthorized: false },
};

const tgtDbConfig = {
    host: process.env.DB_HOST,
    port: Number(process.env.PORT || 5432),
    database: process.env.DB_TARGET,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    ssl: { rejectUnauthorized: false },
};

const ALLOWED_TABLES = new Set<string>([
    // "catalog",
    // "spec_characteristic",
    // "product_specification",
    // "offering_category",
    // "product_offering",
    // "material",
    // "offering_characteristic",
    // "price_component",
    // "price_value",
    // "rel_c2c",
    // "rel_c2o",
    // "rel_o2o",
]); // empty = all
const INTERNAL_TABLES = ["databasechangelog", "databasechangeloglock"];

const TIMESTAMP = new Date().toISOString().replace(/[:.]/g, "-");
const OUTPUT_DIR = `./db/diff/${TIMESTAMP}`;

/* ────────────── YAML HELPERS ────────────── */
const yamlValue = (v: any) =>
    v === null
        ? "null"
        : typeof v === "number" || typeof v === "boolean"
            ? String(v)
            : `'${String(v).replace(/'/g, "''")}'`;

const rowKey = (r: Row, pk: TablePK) =>
    pk.map(k => String(r[k])).join("|");

const whereClause = (r: Row, pk: TablePK) =>
    pk.map(k => `${k} = ${yamlValue(r[k])}`).join(" AND ");

/* ────────────── METADATA LOADERS ────────────── */
async function loadTables(c: Client): Promise<string[]> {
    const r = await c.query(`
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE'
  `);
    return r.rows
        .map(x => x.table_name.toLowerCase())
        .filter(
            t =>
                !INTERNAL_TABLES.includes(t) &&
                (!ALLOWED_TABLES.size || ALLOWED_TABLES.has(t))
        );
}

async function loadPKs(c: Client): Promise<Map<string, TablePK>> {
    const r = await c.query(`
    SELECT tc.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type='PRIMARY KEY'
      AND tc.table_schema='public'
    ORDER BY kcu.ordinal_position
  `);
    const m = new Map<string, TablePK>();
    r.rows.forEach(x => {
        const t = x.table_name.toLowerCase();
        if (!m.has(t)) m.set(t, []);
        m.get(t)!.push(x.column_name);
    });
    return m;
}

async function loadFKs(c: Client): Promise<FK[]> {
    const r = await c.query(`
    SELECT tc.table_name child, ccu.table_name parent
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name
    WHERE tc.constraint_type='FOREIGN KEY'
      AND tc.table_schema='public'
  `);
    return r.rows.map(x => ({
        childTable: x.child.toLowerCase(),
        parentTable: x.parent.toLowerCase(),
    }));
}

/* ────────────── FK TOPO SORT ────────────── */
function topoSortTables(tables: string[], fks: FK[]): string[] {
    const g = new Map<string, Set<string>>();
    tables.forEach(t => g.set(t, new Set()));
    fks.forEach(fk => g.get(fk.childTable)?.add(fk.parentTable));

    const res: string[] = [];
    const v = new Set<string>();
    const visiting = new Set<string>();

    const dfs = (t: string) => {
        if (visiting.has(t)) throw new Error(`FK cycle at ${t}`);
        if (!v.has(t)) {
            visiting.add(t);
            g.get(t)!.forEach(dfs);
            visiting.delete(t);
            v.add(t);
            res.push(t);
        }
    };

    tables.forEach(dfs);
    return res; // parent → child
}

/* ────────────── CHANGESET BUILDERS ────────────── */
const insertCS = (t: string, r: Row, pk: TablePK) => `
- changeSet:
    id: ${t}-insert-${rowKey(r, pk)}
    author: auto
    changes:
      - insert:
          tableName: ${t}
          columns:
${Object.entries(r)
        .map(
            ([c, v]) =>
                `            - column:
                name: ${c}
                value: ${yamlValue(v)}`
        )
        .join("\n")}`;

const updateCS = (t: string, r: Row, cols: string[], pk: TablePK) => `
- changeSet:
    id: ${t}-update-${rowKey(r, pk)}
    author: auto
    changes:
      - update:
          tableName: ${t}
          columns:
${cols
        .map(
            c =>
                `            - column:
                name: ${c}
                value: ${yamlValue(r[c])}`
        )
        .join("\n")}
          where: ${whereClause(r, pk)}`;

const deleteCS = (t: string, r: Row, pk: TablePK) => `
- changeSet:
    id: ${t}-delete-${rowKey(r, pk)}
    author: auto
    changes:
      - delete:
          tableName: ${t}
          where: ${whereClause(r, pk)}`;

/* ────────────── DIFF ENGINE ────────────── */
async function diffTable(
    t: string,
    pk: TablePK,
    ref: Client,
    tgt: Client
): Promise<TableDiff | null> {
    if (!pk?.length) return null;

    const refRows = (await ref.query(`SELECT * FROM ${t}`)).rows;
    let tgtRows: Row[] = [];
    try {
        tgtRows = (await tgt.query(`SELECT * FROM ${t}`)).rows;
    } catch { }

    const rMap = new Map(refRows.map(r => [rowKey(r, pk), r]));
    const tMap = new Map(tgtRows.map(r => [rowKey(r, pk), r]));

    const inserts: string[] = [];
    const updates: string[] = [];
    const deletes: string[] = [];

    for (const r of refRows)
        if (!tMap.has(rowKey(r, pk)))
            inserts.push(insertCS(t, r, pk));

    for (const r of refRows) {
        const tRow = tMap.get(rowKey(r, pk));
        if (!tRow) continue;
        const changed = Object.keys(r).filter(
            c => String(r[c]) !== String(tRow[c])
        );
        if (changed.length)
            updates.push(updateCS(t, r, changed, pk));
    }

    for (const r of tgtRows)
        if (!rMap.has(rowKey(r, pk)))
            deletes.push(deleteCS(t, r, pk));

    if (!inserts.length && !updates.length && !deletes.length) return null;
    return { table: t, inserts, updates, deletes };
}

/* ────────────── MAIN ────────────── */
async function run() {
    const ref = new Client(refDbConfig);
    const tgt = new Client(tgtDbConfig);
    await ref.connect();
    await tgt.connect();

    const tables = await loadTables(ref);
    const targetTables = await loadTables(tgt);
    const existing = tables.filter(t => targetTables.includes(t));

    const pks = await loadPKs(ref);
    const fks = await loadFKs(ref);

    const order = topoSortTables(existing, fks);
    const reverse = [...order].reverse();

    const diffs = new Map<string, TableDiff>();
    for (const t of order) {
        const d = await diffTable(t, pks.get(t)!, ref, tgt);
        if (d) diffs.set(t, d);
    }

    const hasChanges = [...diffs.values()].some(
        d => d.inserts.length || d.updates.length || d.deletes.length
    );

    if (!hasChanges) {
        console.log("✅ No data differences detected. Nothing generated.");
        await ref.end();
        await tgt.end();
        return;
    }

    const del: string[] = [];
    const upd: string[] = [];
    const ins: string[] = [];

    reverse.forEach(t => diffs.get(t)?.deletes.forEach(x => del.push(x)));
    order.forEach(t => {
        diffs.get(t)?.updates.forEach(x => upd.push(x));
        diffs.get(t)?.inserts.forEach(x => ins.push(x));
    });

    fs.mkdirSync(OUTPUT_DIR, { recursive: true });

    if (del.length)
        fs.writeFileSync(
            `${OUTPUT_DIR}/deletes.yaml`,
            `databaseChangeLog:\n${del.join("\n")}`
        );

    if (upd.length)
        fs.writeFileSync(
            `${OUTPUT_DIR}/updates.yaml`,
            `databaseChangeLog:\n${upd.join("\n")}`
        );

    if (ins.length)
        fs.writeFileSync(
            `${OUTPUT_DIR}/inserts.yaml`,
            `databaseChangeLog:\n${ins.join("\n")}`
        );

    const includes = [];
    if (del.length) includes.push("deletes.yaml");
    if (upd.length) includes.push("updates.yaml");
    if (ins.length) includes.push("inserts.yaml");

    fs.writeFileSync(
        `${OUTPUT_DIR}/master-changelog.yaml`,
        `databaseChangeLog:\n${includes
            .map(f => `  - include: { file: ${f} }`)
            .join("\n")}`
    );

    await snapshot();
    await ref.end();
    await tgt.end();
    if (output) {
        fs.appendFileSync(output, `diffPath=${TIMESTAMP}\n`);
    }
    console.log("✅ Diff generated");
}

/* ────────────── SNAPSHOT ────────────── */
async function snapshot() {
    try {
        const liquibase = new Liquibase({
            ...POSTGRESQL_DEFAULT_CONFIG,
            url: `jdbc:postgresql://${process.env.DB_HOST}:${process.env.PORT}/${process.env.DB_TARGET}`,
            username: process.env.DB_USERNAME!,
            password: process.env.DB_PASSWORD!,
        });

        await liquibase.generateChangeLog({
            diffTypes:
                "data,table,column,primaryKey,foreignKey,index,uniqueConstraint",
            changelogFile: `${OUTPUT_DIR}/snapshot.yaml`,
        });
    } catch (e) {
        error("Snapshot failed", e);
    }
}

run().catch(e => {
    console.error("❌ Failed:", e);
    process.exit(1);
});
