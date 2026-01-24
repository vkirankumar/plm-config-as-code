import { Client, types } from "pg";
import fs from "fs";
import path from "path";
import { configDotenv } from "dotenv";
import { Liquibase, POSTGRESQL_DEFAULT_CONFIG } from "liquibase";

types.setTypeParser(1082, (v: string) => v); // DATE as string
configDotenv();
const output = process.env.GITHUB_OUTPUT;

/* ───────────── CONFIG ───────────── */
const TS = new Date().toISOString().replace(/[:.]/g, "-");
const OUT = path.resolve(`db/diff/${TS}`);

const DB_BASE = {
  host: process.env.DB_HOST,
  port: Number(process.env.PORT ?? 5432),
  user: process.env.DB_USERNAME!,
  password: process.env.DB_PASSWORD!,
  ssl: { rejectUnauthorized: false },
};

const DB_REF = { ...DB_BASE, database: process.env.DB_REFERENCE };
const DB_TGT = { ...DB_BASE, database: process.env.DB_TARGET };

const INTERNAL_TABLES = new Set(["databasechangelog", "databasechangeloglock"]);

/* ───────────── HELPERS ───────────── */
const wrap = (lines: string[]) => `databaseChangeLog:\n${lines.join("\n")}\n`;
const yamlVal = (v: any) =>
  v === null
    ? "null"
    : typeof v === "number" || typeof v === "boolean"
    ? v
    : `'${String(v).replace(/'/g, "''")}'`;

const rowKey = (row: Record<string, any>, pk: string[]) =>
  pk.map(k => String(row[k])).join("|");

const whereClause = (row: Record<string, any>, pk: string[]) =>
  pk.map(k => `${k} = ${yamlVal(row[k])}`).join(" AND ");

function toArray(v: any): string[] {
  if (Array.isArray(v)) return v;
  if (typeof v === "string") return v.replace(/[{}"]/g, "").split(",").filter(Boolean);
  return [];
}

/* ───────────── METADATA ───────────── */
async function tables(c: Client): Promise<string[]> {
  const r = await c.query(`
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
      AND table_type='BASE TABLE'
  `);
  return r.rows.map(r => r.table_name).filter(t => !INTERNAL_TABLES.has(t));
}

async function pks(c: Client): Promise<Map<string, string[]>> {
  const r = await c.query(`
    SELECT tc.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type='PRIMARY KEY'
  `);
  const m = new Map<string, string[]>();
  r.rows.forEach(r => {
    if (!m.has(r.table_name)) m.set(r.table_name, []);
    m.get(r.table_name)!.push(r.column_name);
  });
  return m;
}

type FK = {
  name: string;
  child: string;
  parent: string;
  childCols: string[];
  parentCols: string[];
};

async function fks(c: Client): Promise<FK[]> {
  const r = await c.query(`
    SELECT
      con.conname,
      src.relname AS child,
      tgt.relname AS parent,
      array_agg(sa.attname ORDER BY s.pos) AS child_cols,
      array_agg(ta.attname ORDER BY s.pos) AS parent_cols
    FROM pg_constraint con
    JOIN pg_class src ON src.oid = con.conrelid
    JOIN pg_class tgt ON tgt.oid = con.confrelid
    CROSS JOIN LATERAL unnest(con.conkey)
      WITH ORDINALITY AS s(attnum, pos)
    CROSS JOIN LATERAL unnest(con.confkey)
      WITH ORDINALITY AS t(attnum, pos)
    JOIN pg_attribute sa
      ON sa.attrelid = src.oid AND sa.attnum = s.attnum
    JOIN pg_attribute ta
      ON ta.attrelid = tgt.oid AND ta.attnum = t.attnum
    WHERE con.contype = 'f'
      AND s.pos = t.pos
    GROUP BY con.conname, src.relname, tgt.relname
  `);

  return r.rows.map(r => ({
    name: r.conname,
    child: r.child,
    parent: r.parent,
    childCols: toArray(r.child_cols),
    parentCols: toArray(r.parent_cols),
  }));
}

/* ───────────── TOPO SORT ───────────── */
function topoSort(tables: string[], fks: FK[], reverse = false): string[] {
  const g = new Map<string, Set<string>>();
  tables.forEach(t => g.set(t, new Set()));
  fks.forEach(f => {
    if (!g.has(f.child) || !g.has(f.parent)) return;
    reverse
      ? g.get(f.parent)!.add(f.child) // children first for delete
      : g.get(f.child)!.add(f.parent); // parents first for insert
  });
  const seen = new Set<string>();
  const out: string[] = [];
  function visit(t: string) {
    if (seen.has(t)) return;
    seen.add(t);
    g.get(t)?.forEach(visit);
    out.push(t);
  }
  tables.forEach(visit);
  return out;
}

/* ───────────── ROW DIFFS ───────────── */
async function generateRowDiff(
  table: string,
  pk: string[],
  ref: Client,
  tgt: Client
): Promise<{ inserts: string[]; updates: string[]; deletes: string[] }> {
  const refRows = (await ref.query(`SELECT * FROM ${table}`)).rows;
  const tgtRows = (await tgt.query(`SELECT * FROM ${table}`)).rows;
  const refMap = new Map(refRows.map(r => [rowKey(r, pk), r]));
  const tgtMap = new Map(tgtRows.map(r => [rowKey(r, pk), r]));

  const inserts: string[] = [];
  const updates: string[] = [];
  const deletes: string[] = [];

  // Deletes
  tgtRows.filter(r => !refMap.has(rowKey(r, pk))).forEach(r => {
    deletes.push(`
- changeSet:
    id: delete-${table}-${rowKey(r, pk)}
    author: auto
    changes:
      - delete:
          tableName: ${table}
          where: ${whereClause(r, pk)}
`.trim());
  });

  // Inserts
  refRows.filter(r => !tgtMap.has(rowKey(r, pk))).forEach(r => {
    inserts.push(`
- changeSet:
    id: insert-${table}-${rowKey(r, pk)}
    author: auto
    changes:
      - insert:
          tableName: ${table}
          columns:
${Object.entries(r)
        .map(([c, v]) => `            - column:\n                name: ${c}\n                value: ${yamlVal(v)}`)
        .join("\n")}
`.trim());
  });

  // Updates
  refRows.filter(r => tgtMap.has(rowKey(r, pk))).forEach(r => {
    const tRow = tgtMap.get(rowKey(r, pk))!;
    const changedCols = Object.entries(r).filter(([c, v]) => String(tRow[c]) !== String(v));
    if (!changedCols.length) return;
    updates.push(`
- changeSet:
    id: update-${table}-${rowKey(r, pk)}
    author: auto
    changes:
      - update:
          tableName: ${table}
          columns:
${changedCols.map(([c, v]) => `            - column:\n                name: ${c}\n                value: ${yamlVal(v)}`).join("\n")}
          where: ${whereClause(r, pk)}
`.trim());
  });

  return { inserts, updates, deletes };
}

/* ───────────── SNAPSHOT ───────────── */
async function generateSnapshot(outDir: string) {
  const lb = new Liquibase({
    ...POSTGRESQL_DEFAULT_CONFIG,
    url: `jdbc:postgresql://${process.env.DB_HOST}:${process.env.PORT}/${process.env.DB_TARGET}`,
    username: process.env.DB_USERNAME!,
    password: process.env.DB_PASSWORD!,
  });
  await lb.generateChangeLog({
    changelogFile: `${outDir}/snapshot.yaml`,
    diffTypes: "table,column,primaryKey,foreignKey,index,uniqueConstraint,data",
  });
}

/* ───────────── MAIN ───────────── */
async function run() {
  const ref = new Client(DB_REF);
  const tgt = new Client(DB_TGT);
  await ref.connect();
  await tgt.connect();

  const [refTables, tgtTables, refFKs, tgtFKs, pkMap] = await Promise.all([
    tables(ref),
    tables(tgt),
    fks(ref),
    fks(tgt),
    pks(ref)
  ]);

  let hasChanges = false;
  const files: { name: string; lines: string[] }[] = [];

  /* DROPPED TABLES */
  let dropped = tgtTables.filter(t => !refTables.includes(t));
  dropped = topoSort(dropped, tgtFKs, true); // children first
  if (dropped.length) {
    hasChanges = true;
    files.push({ name: "drop-tables.yaml", lines: dropped.map(t => `
- changeSet:
    id: drop-${t}
    author: auto
    changes:
      - dropTable:
          tableName: ${t}
`.trim()) });
  }

  /* NEW TABLES */
  let created = refTables.filter(t => !tgtTables.includes(t));
  created = topoSort(created, refFKs, false); // parents first
  if (created.length) {
    hasChanges = true;
    files.push({ name: "new-tables.yaml", lines: created.map(t => `
- changeSet:
    id: create-${t}
    author: auto
    changes:
      - createTable:
          tableName: ${t}
`.trim()) });
  }

  /* NEW FKs */
  const tgtFKMap = new Set(
    tgtFKs.map(f => `${f.child}|${f.parent}|${f.childCols.join(",")}`)
  );
  const newFKs = refFKs.filter(
    f => !tgtFKMap.has(`${f.child}|${f.parent}|${f.childCols.join(",")}`)
  );
  if (newFKs.length) {
    hasChanges = true;
    files.push({
      name: "new-fks.yaml",
      lines: newFKs.map(f => `
- changeSet:
    id: fk-${f.name}
    author: auto
    changes:
      - addForeignKeyConstraint:
          constraintName: ${f.name}
          baseTableName: ${f.child}
          baseColumnNames: ${f.childCols.join(",")}
          referencedTableName: ${f.parent}
          referencedColumnNames: ${f.parentCols.join(",")}
`.trim())
    });
  }

  /* ROW-LEVEL DIFFS */
  const existingTables = refTables.filter(t => tgtTables.includes(t));

  // DELETE first, FK-safe (children first)
  for (const t of topoSort(existingTables, refFKs, true)) {
    const pk = pkMap.get(t) ?? [];
    const { deletes } = await generateRowDiff(t, pk, ref, tgt);
    if (deletes.length) {
      hasChanges = true;
      files.push({ name: `delete-${t}.yaml`, lines: deletes });
    }
  }

  // INSERT next, FK-safe (parents first)
  for (const t of topoSort(existingTables, refFKs, false)) {
    const pk = pkMap.get(t) ?? [];
    const { inserts, updates } = await generateRowDiff(t, pk, ref, tgt);
    if (inserts.length || updates.length) {
      hasChanges = true;
      files.push({ name: `diff-${t}.yaml`, lines: [...inserts, ...updates] });
    }
  }

  /* WRITE FILES */
  if (hasChanges) {
    fs.mkdirSync(OUT, { recursive: true });
    for (const f of files) fs.writeFileSync(path.join(OUT, f.name), wrap(f.lines));
    fs.writeFileSync(
      path.join(OUT, "master-changelog.yaml"),
      wrap(files.map(f => `  - include:\n      file: ./db/diff/${TS}/${f.name}`))
    );
    await generateSnapshot(OUT);
    if (output) fs.appendFileSync(output, `diffPath=${TS}\n`);
    console.log("✅ Diff generated:", OUT);
  } else {
    console.log("✅ Databases already in sync");
  }

  await ref.end();
  await tgt.end();
}

run().catch(e => {
  console.error(e);
  process.exit(1);
});
