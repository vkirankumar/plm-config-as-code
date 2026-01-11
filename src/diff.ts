import { Client, types } from "pg";
import fs from "fs";
import path from "path";
import { configDotenv } from "dotenv";
import { Liquibase, POSTGRESQL_DEFAULT_CONFIG } from "liquibase";

types.setTypeParser(1082, (val: string) => val); // DATE
const output = process.env.GITHUB_OUTPUT;
configDotenv();

/* ───────────── CONFIG ───────────── */
const TS = new Date().toISOString().replace(/[:.]/g, "-");
const OUT = path.resolve(`db/diff/${TS}`);

const DB_REF = {
  host: process.env.DB_HOST,
  port: Number(process.env.PORT ?? 5432),
  database: process.env.DB_REFERENCE,
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  ssl: { rejectUnauthorized: false },
};

const DB_TGT = { ...DB_REF, database: process.env.DB_TARGET };

/* ───────────── INTERNAL TABLES ───────────── */
const INTERNAL_TABLES = new Set(["databasechangelog", "databasechangeloglock"]);

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

/* ───────────── HELPERS ───────────── */
const wrap = (lines: string[]) => `databaseChangeLog:\n${lines.join("\n")}\n`;
const yamlVal = (v: any) =>
  v === null
    ? "null"
    : typeof v === "number"
      ? v
      : `'${String(v).replace(/'/g, "''")}'`;
const rowKey = (row: any, pk: string[]) => pk.map((k) => row[k]).join("|");
const whereClause = (row: any, pk: string[]) =>
  pk.map((k) => `${k} = ${yamlVal(row[k])}`).join(" AND ");

/* ───────────── METADATA ───────────── */
async function tables(c: Client) {
  return (
    await c.query(`
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE'
  `)
  ).rows
    .map((r) => r.table_name)
    .filter((t) => !INTERNAL_TABLES.has(t));
}

async function columns(c: Client, t: string) {
  return (
    await c.query(`
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name='${t}'
  `)
  ).rows;
}

async function pks(c: Client) {
  const m = new Map<string, string[]>();
  (
    await c.query(`
    SELECT tc.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type='PRIMARY KEY'
  `)
  ).rows.forEach((r) => {
    if (!m.has(r.table_name)) m.set(r.table_name, []);
    m.get(r.table_name)!.push(r.column_name);
  });
  return m;
}

async function fks(c: Client) {
  return (
    await c.query(`
    SELECT tc.table_name child, ccu.table_name parent
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name
    WHERE tc.constraint_type='FOREIGN KEY'
  `)
  ).rows.filter(
    (f) => !INTERNAL_TABLES.has(f.child) && !INTERNAL_TABLES.has(f.parent)
  );
}

/* ───────────── FK-SAFE TOPO SORT ───────────── */
function topoSort(
  tables: string[],
  fks: { child: string; parent: string }[],
  dropOrder = false
) {
  const graph = new Map<string, Set<string>>();
  tables.forEach((t) => graph.set(t, new Set()));

  fks.forEach((fk) => {
    if (!graph.has(fk.child) || !graph.has(fk.parent)) return;
    if (dropOrder) graph.get(fk.parent)?.add(fk.child); // reverse for drop
    else graph.get(fk.child)?.add(fk.parent); // normal for insert
  });

  const visited = new Set<string>();
  const result: string[] = [];
  function visit(t: string) {
    if (!visited.has(t)) {
      visited.add(t);
      graph.get(t)?.forEach(visit);
      result.push(t);
    }
  }
  tables.forEach(visit);
  return result;
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

  const refTables = await tables(ref);
  const tgtTables = await tables(tgt);
  const pkMap = await pks(ref);
  const fkList = await fks(ref);

  let hasChanges = false;
  const fileContents: { name: string; lines: string[] }[] = [];

  /* ───── DROPPED TABLES ───── */
  let dropped = tgtTables.filter((t) => !refTables.includes(t));
  dropped = topoSort(dropped, fkList, true); // children first
  if (dropped.length) {
    hasChanges = true;
    fileContents.push({
      name: "drop-tables.yaml",
      lines: dropped.map((t) =>
        `
- changeSet:
    id: drop-${t}
    author: auto
    preConditions:
      - tableExists:
          tableName: ${t}
    changes:
      - dropTable:
          tableName: ${t}
`.trim()
      ),
    });
  }

  /* ───── NEW TABLES + ROW INSERTS ───── */
  let newTables = refTables.filter((t) => !tgtTables.includes(t));
  newTables = topoSort(newTables, fkList, false); // parents first
  const newTableCS: string[] = [];
  const newFKCS: string[] = [];

  for (const t of newTables) {
    hasChanges = true;
    const cols = await columns(ref, t);
    const pk = pkMap.get(t) ?? [];

    // 1️⃣ create table + PK
    newTableCS.push(
      `
- changeSet:
    id: create-${t}
    author: auto
    changes:
      - createTable:
          tableName: ${t}
          columns:
${cols
          .map(
            (c) => `
            - column:
                name: ${c.column_name}
                type: ${c.data_type}
                constraints:
                  nullable: ${c.is_nullable === "YES"}`
          )
          .join("\n")}
${pk.length
          ? `
      - addPrimaryKey:
          tableName: ${t}
          columnNames: ${pk.join(", ")}`
          : ""
        }
`.trim()
    );

    // 2️⃣ add row inserts for new table
    const rows = (await ref.query(`SELECT * FROM ${t}`)).rows;
    rows.forEach((r) => {
      newTableCS.push(
        `
- changeSet:
    id: insert-${t}-${rowKey(r, pk)}
    author: auto
    changes:
      - insert:
          tableName: ${t}
          columns:
${Object.entries(r)
            .map(
              ([c, v]) =>
                `            - column:\n                name: ${c}\n                value: ${yamlVal(
                  v
                )}`
            )
            .join("\n")}
`.trim()
      );
    });
  }

  // 3️⃣ add FK constraints for new tables
  fkList
    .filter((f) => newTables.includes(f.child))
    .forEach((f) => {
      newFKCS.push(
        `
- changeSet:
    id: fk-${f.child}-${f.parent}
    author: auto
    changes:
      - addForeignKeyConstraint:
          baseTableName: ${f.child}
          referencedTableName: ${f.parent}
`.trim()
      );
    });

  if (newTableCS.length)
    fileContents.push({ name: "new-tables.yaml", lines: newTableCS });
  if (newFKCS.length)
    fileContents.push({ name: "new-fks.yaml", lines: newFKCS });

  /* ───── ROW-LEVEL DATA DIFF ───── */
  const diffs: string[] = [];
  const existingTables = refTables.filter((t) => tgtTables.includes(t));
  const deleteOrder = topoSort(existingTables, fkList, true); // children first
  const insertOrder = topoSort(existingTables, fkList, false); // parents first

  // DELETE rows first
  for (const t of deleteOrder) {
    const pk = pkMap.get(t);
    if (!pk?.length) continue;
    const refRows = (await ref.query(`SELECT * FROM ${t}`)).rows;
    const tgtRows = (await tgt.query(`SELECT * FROM ${t}`)).rows;
    const refMap = new Map(refRows.map((r) => [rowKey(r, pk), r]));
    const deletes = tgtRows.filter((r) => !refMap.has(rowKey(r, pk)));
    for (const r of deletes) {
      diffs.push(
        `
- changeSet:
    id: delete-${t}-${rowKey(r, pk)}
    author: auto
    changes:
      - delete:
          tableName: ${t}
          where: ${whereClause(r, pk)}
`.trim()
      );
    }
  }

  // INSERT + UPDATE for existing tables
  for (const t of insertOrder) {
    const pk = pkMap.get(t);
    if (!pk?.length) continue;
    const refRows = (await ref.query(`SELECT * FROM ${t}`)).rows;
    const tgtRows = (await tgt.query(`SELECT * FROM ${t}`)).rows;
    const tgtMap = new Map(tgtRows.map((r) => [rowKey(r, pk), r]));

    const inserts = refRows.filter((r) => !tgtMap.has(rowKey(r, pk)));
    for (const r of inserts) {
      diffs.push(
        `
- changeSet:
    id: insert-${t}-${rowKey(r, pk)}
    author: auto
    changes:
      - insert:
          tableName: ${t}
          columns:
${Object.entries(r)
            .map(
              ([c, v]) =>
                `            - column:\n                name: ${c}\n                value: ${yamlVal(
                  v
                )}`
            )
            .join("\n")}
`.trim()
      );
    }

    const updates = refRows.filter((r) => {
      const tRow = tgtMap.get(rowKey(r, pk));
      return (
        tRow && Object.keys(r).some((c) => String(r[c]) !== String(tRow[c]))
      );
    });
    for (const r of updates) {
      const tRow = tgtMap.get(rowKey(r, pk))!;
      const changedCols = Object.entries(r).filter(
        ([c, v]) => String(tRow[c]) !== String(v)
      );
      diffs.push(
        `
- changeSet:
    id: update-${t}-${rowKey(r, pk)}
    author: auto
    changes:
      - update:
          tableName: ${t}
          columns:
${changedCols
            .map(
              ([c, v]) =>
                `            - column:\n                name: ${c}\n                value: ${yamlVal(
                  v
                )}`
            )
            .join("\n")}
          where: ${whereClause(r, pk)}
`.trim()
      );
    }
  }

  if (diffs.length) fileContents.push({ name: "diffs.yaml", lines: diffs });
  if (fileContents.length) hasChanges = true;

  /* ───── WRITE FILES ONLY IF CHANGES ───── */
  if (hasChanges) {
    fs.mkdirSync(OUT, { recursive: true });
    for (const f of fileContents)
      fs.writeFileSync(path.join(OUT, f.name), wrap(f.lines));

    // master changelog
    fs.writeFileSync(
      path.join(OUT, "master-changelog.yaml"),
      wrap(fileContents.map((f) => `  - include: 
      file: ./db/diff/${TS}/${f.name}`))
    );

    // snapshot
    await generateSnapshot(OUT);
    console.log("✅ Diff generated in folder:", OUT);
    if (output) {
      fs.appendFileSync(output, `diffPath=${TS}\n`);
    }
  } else {
    console.log("✅ Databases in sync! No changes detected.");
  }

  await ref.end();
  await tgt.end();
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
