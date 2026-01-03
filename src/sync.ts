import { Client, types } from "pg";
import * as fs from "fs";
import * as path from "path";
import { configDotenv } from "dotenv";
import { Liquibase, POSTGRESQL_DEFAULT_CONFIG } from "liquibase";
import { error } from "console";

types.setTypeParser(1082, (val: string) => val); // DATE

// ────────────── TYPES ──────────────
type Row = Record<string, any>;
type TablePK = string[];
type FK = { childTable: string; parentTable: string };
type TableDiff = { table: string; yaml: string };
const output = process.env.GITHUB_OUTPUT;

configDotenv();

// ────────────── CONFIG ──────────────
const refDbConfig = {
  host: process.env.DB_HOST,
  port: process.env.PORT ? parseInt(process.env.PORT) : 5432,
  database: process.env.DB_REFERENCE,
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  ssl: { rejectUnauthorized: false },
};

const targetDbConfig = {
  host: process.env.DB_HOST,
  port: process.env.PORT ? parseInt(process.env.PORT) : 5432,
  database: process.env.DB_TARGET,
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  ssl: { rejectUnauthorized: false },
};

// Only these tables will be included. Empty = all tables.
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
]);

const timeStamp: string = new Date().toISOString().replace(/[:.&]/g, "-");
const OUTPUT_DIR = `./db/diff/${timeStamp}/data`;
const INTERNAL_TABLES = ["databasechangelog", "databasechangeloglock"];

// ────────────── HELPERS ──────────────
function yamlValue(v: any): string {
  if (v === null) return "null";
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return `'${String(v).replace(/'/g, "''")}'`;
}

function rowKey(row: Row, pk: TablePK): string {
  return pk.map((c) => String(row[c])).join("|");
}

function whereClause(row: Row, pk: TablePK): string {
  return pk.map((c) => `${c} = ${yamlValue(row[c])}`).join(" AND ");
}

// ────────────── DB METADATA LOADERS ──────────────
async function loadTables(client: Client): Promise<string[]> {
  const res = await client.query(`
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE'
  `);

  return res.rows
    .map((r) => r.table_name.toLowerCase())
    .filter(
      (t) =>
        !INTERNAL_TABLES.includes(t) &&
        (ALLOWED_TABLES.size === 0 || ALLOWED_TABLES.has(t))
    );
}

async function loadPKs(client: Client): Promise<Map<string, TablePK>> {
  const res = await client.query(`
    SELECT tc.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type='PRIMARY KEY'
      AND tc.table_schema='public'
    ORDER BY kcu.ordinal_position
  `);

  const map = new Map<string, string[]>();
  res.rows.forEach((r) => {
    const t = r.table_name.toLowerCase();
    if (!map.has(t)) map.set(t, []);
    map.get(t)!.push(r.column_name.toLowerCase());
  });
  return map;
}

async function loadFKs(client: Client): Promise<FK[]> {
  const res = await client.query(`
    SELECT tc.table_name AS child,
           ccu.table_name AS parent
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name
    WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema='public'
  `);

  return res.rows
    .map((r) => ({
      childTable: r.child.toLowerCase(),
      parentTable: r.parent.toLowerCase(),
    }))
    .filter(
      (fk) => ALLOWED_TABLES.size === 0 || ALLOWED_TABLES.has(fk.childTable)
    );
}

// ────────────── FK-SAFE TOPO SORT ──────────────
function topoSortTables(tables: string[], fks: FK[]): string[] {
  const graph = new Map<string, Set<string>>();
  tables.forEach((t) => graph.set(t, new Set()));

  fks.forEach((fk) => {
    graph.get(fk.childTable)?.add(fk.parentTable);
  });

  const visited = new Set<string>();
  const visiting = new Set<string>();
  const result: string[] = [];

  function visit(t: string) {
    if (visiting.has(t)) throw new Error(`FK cycle detected: ${t}`);
    if (!visited.has(t)) {
      visiting.add(t);
      graph.get(t)!.forEach(visit);
      visiting.delete(t);
      visited.add(t);
      result.push(t);
    }
  }

  tables.forEach(visit);
  return result;
}

// ────────────── GENERATE NEW TABLE CHANGELOG ──────────────
async function generateNewTableChangeLog(
  table: string,
  pk: TablePK,
  ref: Client
): Promise<TableDiff> {
  const colRes = await ref.query(`
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name='${table}'
    ORDER BY ordinal_position
  `);

  const columns = colRes.rows.map((r) => ({
    name: r.column_name,
    type: r.data_type,
    nullable: r.is_nullable === "YES",
  }));

  const lines: string[] = [];

  // createTable
  lines.push(`
- changeSet:
    id: ${table}-create
    author: auto
    changes:
      - createTable:
          tableName: ${table}
          columns:
${columns
  .map(
    (c) =>
      `            - column:\n                name: ${
        c.name
      }\n                type: ${c.type}${
        c.nullable
          ? ""
          : "\n                constraints:\n                  nullable: false"
      }`
  )
  .join("\n")}`);

  // primary key
  if (pk.length > 0) {
    lines.push(`
      - addPrimaryKey:
          tableName: ${table}
          columnNames: ${pk.join(", ")}`);
  }

  // row inserts
  const refRows: Row[] = (await ref.query(`SELECT * FROM ${table}`)).rows;
  refRows.forEach((r) => {
    lines.push(`
- changeSet:
    id: ${table}-insert-${rowKey(r, pk)}
    author: auto
    changes:
      - insert:
          tableName: ${table}
          columns:
${Object.entries(r)
  .map(
    ([c, v]) =>
      `            - column:\n                name: ${c}\n                value: ${yamlValue(
        v
      )}`
  )
  .join("\n")}`);
  });

  return {
    table,
    yaml: `databaseChangeLog:\n${lines.join("\n")}`.trim() + "\n",
  };
}

// ────────────── GENERATE TABLE DIFF ──────────────
async function generateTableDiff(
  table: string,
  pk: TablePK,
  ref: Client,
  tgt: Client
): Promise<TableDiff | null> {
  if (!pk || pk.length === 0) {
    console.warn(`⚠️ Table "${table}" has no primary key. Skipping.`);
    return null;
  }

  const refRows: Row[] = (await ref.query(`SELECT * FROM ${table}`)).rows;
  let tgtRows: Row[] = [];
  try {
    tgtRows = (await tgt.query(`SELECT * FROM ${table}`)).rows;
  } catch {}

  const refMap = new Map(refRows.map((r) => [rowKey(r, pk), r]));
  const tgtMap = new Map(tgtRows.map((r) => [rowKey(r, pk), r]));

  const inserts = refRows.filter((r) => !tgtMap.has(rowKey(r, pk)));
  const deletes = tgtRows.filter((r) => !refMap.has(rowKey(r, pk)));
  const updates = refRows.filter((r) => {
    const t = tgtMap.get(rowKey(r, pk));
    return t && Object.keys(r).some((c) => String(r[c]) !== String(t[c]));
  });

  if (!inserts.length && !updates.length && !deletes.length) return null;

  const lines: string[] = [];
  for (const r of inserts)
    lines.push(`
- changeSet:
    id: ${table}-insert-${rowKey(r, pk)}
    author: auto
    changes:
      - insert:
          tableName: ${table}
          columns:
${Object.entries(r)
  .map(
    ([c, v]) =>
      `            - column:\n                name: ${c}\n                value: ${yamlValue(
        v
      )}`
  )
  .join("\n")}`);

  for (const r of updates) {
    const t = tgtMap.get(rowKey(r, pk))!;
    const changed = Object.entries(r).filter(
      ([c, v]) => String(v) !== String(t[c])
    );
    lines.push(`
- changeSet:
    id: ${table}-update-${rowKey(r, pk)}
    author: auto
    changes:
      - update:
          tableName: ${table}
          columns:
${changed
  .map(
    ([c, v]) =>
      `            - column:\n                name: ${c}\n                value: ${yamlValue(
        v
      )}`
  )
  .join("\n")}
          where: ${whereClause(r, pk)}`);
  }

  for (const r of deletes)
    lines.push(`
- changeSet:
    id: ${table}-delete-${rowKey(r, pk)}
    author: auto
    changes:
      - delete:
          tableName: ${table}
          where: ${whereClause(r, pk)}`);

  return {
    table,
    yaml: `databaseChangeLog:\n${lines.join("\n")}`.trim() + "\n",
  };
}

// ────────────── MAIN ──────────────
async function run() {
  const ref = new Client(refDbConfig);
  const tgt = new Client(targetDbConfig);
  await ref.connect();
  await tgt.connect();

  const refTables = await loadTables(ref);
  const targetTables = await loadTables(tgt);
  const pkMap = await loadPKs(ref);
  const fks = await loadFKs(ref);

  const existingTables = refTables.filter((t) => targetTables.includes(t));
  const newTables = refTables.filter((t) => !targetTables.includes(t));

  const orderedTables = topoSortTables(existingTables, fks);
  console.log("✅ FK-safe table order:", orderedTables.join(", "));

  const diffs: TableDiff[] = [];

  // existing table diffs
  for (const t of orderedTables) {
    const pk = pkMap.get(t);
    const diff = await generateTableDiff(t, pk!, ref, tgt);
    if (diff) diffs.push(diff);
  }

  // new tables: create table + insert rows
  for (const t of newTables) {
    const pk = pkMap.get(t) || [];
    const diff = await generateNewTableChangeLog(t, pk, ref);
    diffs.push(diff);
  }

  if (!diffs.length) {
    console.log("✅ No changes detected. Nothing generated.");
    await ref.end();
    await tgt.end();
    return;
  }

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  for (const d of diffs)
    fs.writeFileSync(path.join(OUTPUT_DIR, `${d.table}-data.yaml`), d.yaml);

  const master = [
    "databaseChangeLog:",
    ...diffs.map(
      (d) => `  - include:\n      file: ${OUTPUT_DIR}/${d.table}-data.yaml`
    ),
  ].join("\n");
  await generateDBSnapshot();
  fs.writeFileSync(path.join(OUTPUT_DIR, "master-changelog.yaml"), master);
  if (output) {
    fs.appendFileSync(output, `diffPath=${timeStamp}\n`);
  }
  await ref.end();
  await tgt.end();
  console.log("✅ Master changelog and snapshot generated.");
}

run().catch((err) => {
  console.error("❌ Error:", err.message);
  process.exit(1);
});

const generateDBSnapshot = async () => {
  try {
    const diffTypes: string =
      "data,table,column,primaryKey,index,foreignKey,uniqueConstraint";
    const config_dev = {
      ...POSTGRESQL_DEFAULT_CONFIG,
      password: process.env.DB_PASSWORD ?? "",
      username: process.env.DB_USERNAME ?? "",
      url: `jdbc:postgresql://${process.env.DB_HOST}:${process.env.PORT}/${process.env.DB_TARGET}`,
    };
    const liquibase: Liquibase = new Liquibase(config_dev);
    liquibase.generateChangeLog({
      diffTypes,
      changelogFile: `./db/diff/${timeStamp}/snapshot.yaml`,
    });
  } catch (err) {
    error("Failed to create target snapshot!! " + err);
  }
};
