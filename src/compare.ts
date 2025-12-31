import { Client, types } from "pg";
import * as fs from 'fs';

// --- Fix DATE type parsing: return string, not JS Date ---
types.setTypeParser(1082, (val: string) => val); // 1082 = PostgreSQL DATE type

// Configure database connections
const refDbConfig = {
  host: "localhost",
  port: 4001,
  database: "dev",
  user: "postgres",
  password: "postgres",
};

const targetDbConfig = {
  host: "localhost",
  port: 4001,
  database: "prod",
  user: "postgres",
  password: "postgres",
};

// --- Types ---
type Row = { [column: string]: any };
type ColumnInfo = { name: string; data_type: string };
type UpdateRow = { refRow: Row; targetRow: Row };

const timeStamp: string = new Date().toISOString().replace(/[:.&]/g, "-");

// Tables to compare
const tables = [
  "catalog",
  "material",
  "offering_category",
  "offering_characteristic",
  "spec_characteristic",
  "price_component",
  "price_value",
  "rel_c2c",
  "rel_c2o",
  "rel_o2o",
  "product_specification",
  "product_offering",
  "service"
];

const BATCH_SIZE = 50;

// --- Helpers ---
function formatYAMLValue(value: any, dataType: string): string {
  if (value === null) return 'null';
  const numericTypes = ['integer','bigint','smallint','decimal','numeric','real','double precision'];
  const booleanTypes = ['boolean'];
  const dateTypes = ['date'];
  const timestampTypes = ['timestamp without time zone','timestamp with time zone'];

  if (numericTypes.includes(dataType)) return value.toString();
  if (booleanTypes.includes(dataType)) return value ? 'true' : 'false';
  if (dateTypes.includes(dataType)) return `'${value}'`; // value is string
  if (timestampTypes.includes(dataType)) {
    const dt = value instanceof Date ? value : new Date(value);
    const pad = (n: number) => n.toString().padStart(2,'0');
    return `'${dt.getUTCFullYear()}-${pad(dt.getUTCMonth()+1)}-${pad(dt.getUTCDate())} ` +
           `${pad(dt.getUTCHours())}:${pad(dt.getUTCMinutes())}:${pad(dt.getUTCSeconds())}'`;
  }
  return `'${String(value).replace(/'/g,"''")}'`;
}

function normalizeValue(value: any, dataType: string): any {
  if (value === null) return null;
  const numericTypes = ['integer','bigint','smallint','decimal','numeric','real','double precision'];
  const booleanTypes = ['boolean'];
  const dateTypes = ['date'];
  const timestampTypes = ['timestamp without time zone','timestamp with time zone'];

  if (numericTypes.includes(dataType)) return Number(value);
  if (booleanTypes.includes(dataType)) return Boolean(value);
  if (dateTypes.includes(dataType)) return value.toString();
  if (timestampTypes.includes(dataType)) return (value instanceof Date ? value.getTime() : new Date(value).getTime());
  return value.toString();
}

function valuesAreEqual(a: any, b: any, dataType: string): boolean {
  return normalizeValue(a, dataType) === normalizeValue(b, dataType);
}

function pkToString(row: Row, pkColumns: string[]): string {
  return pkColumns.map(c => row[c]).join('|');
}

// --- DB metadata ---
async function getPrimaryKeyColumns(client: Client, table: string): Promise<string[]> {
  const res = await client.query(`
    SELECT a.attname as column_name
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = $1::regclass AND i.indisprimary
  `,[table]);
  return res.rows.map((r:any)=>r.column_name);
}

async function getColumnInfo(client: Client, table: string): Promise<ColumnInfo[]> {
  const res = await client.query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = $1
  `,[table]);
  return res.rows.map((r:any)=>({name:r.column_name, data_type:r.data_type}));
}

async function getSerialColumns(client: Client, table: string): Promise<{ column_name:string, column_default:string }[]> {
  const res = await client.query(`
    SELECT column_name, column_default
    FROM information_schema.columns
    WHERE table_name = $1 AND column_default LIKE 'nextval(%'
  `,[table]);
  return res.rows;
}

// --- YAML generation ---
function generateSequenceYAML(table:string, column:string, sequenceName:string, maxValue:number): string {
  return `- changeSet:
    id: ${table}-sequence-${column}-${Date.now()}
    author: auto-generated
    preConditions:
      onFail: MARK_RAN
    changes:
      - sql:
          sql: SELECT setval('${sequenceName}', ${maxValue+1}, false);`;
}

function generateDeleteYAML(table:string, row:Row, pkColumns:string[]): string {
  const pkCondition = pkColumns.map(c=>`${c} = ${row[c]}`).join(' AND ');
  return `- changeSet:
    id: ${table}-delete-${pkToString(row, pkColumns)}-${Date.now()}
    author: auto-generated
    preConditions:
      onFail: MARK_RAN
      sqlCheck:
        expectedResult: 1
        sql: SELECT COUNT(*) FROM ${table} WHERE ${pkCondition}
    changes:
      - delete:
          tableName: ${table}
          where: ${pkCondition}`;
}

function generateBatchedInsertYAML(table:string, rows:Row[], columns:ColumnInfo[]): string[] {
  const lines:string[] = [];
  for(let i=0;i<rows.length;i+=BATCH_SIZE){
    const batch = rows.slice(i,i+BATCH_SIZE);
    let columnsYaml = '';
    batch.forEach(row=>{
      columns.forEach(col=>{
        columnsYaml += `            - column:\n                name: ${col.name}\n                value: ${formatYAMLValue(row[col.name],col.data_type)}\n`;
      });
    });
    lines.push(`- changeSet:
    id: ${table}-insert-batch-${i}-${Date.now()}
    author: auto-generated
    preConditions:
      onFail: MARK_RAN
    changes:
      - insert:
          tableName: ${table}
          columns:
${columnsYaml}`);
  }
  return lines;
}

function generateBatchedUpdateYAML(table:string, updates:UpdateRow[], pkColumns:string[], columns:ColumnInfo[]): string[] {
  const lines:string[] = [];
  updates.forEach(u=>{
    let columnsYaml = '';
    columns.forEach(col=>{
      if(!valuesAreEqual(u.refRow[col.name],u.targetRow[col.name],col.data_type)){
        columnsYaml += `            - column:\n                name: ${col.name}\n                value: ${formatYAMLValue(u.refRow[col.name], col.data_type)}\n`;
      }
    });
    if(columnsYaml){
      const pkCondition = pkColumns.map(c=>`${c} = ${u.refRow[c]}`).join(' AND ');
      lines.push(`- changeSet:
    id: ${table}-update-${pkToString(u.refRow,pkColumns)}-${Date.now()}
    author: auto-generated
    preConditions:
      onFail: MARK_RAN
      sqlCheck:
        expectedResult: 1
        sql: SELECT COUNT(*) FROM ${table} WHERE ${pkCondition}
    changes:
      - update:
          tableName: ${table}
          columns:
${columnsYaml}          where: ${pkCondition}`);
    }
  });
  return lines;
}

// --- Main generation ---
async function generateDiffPerTable(){
  const refClient = new Client(refDbConfig);
  const targetClient = new Client(targetDbConfig);

  await refClient.connect();
  await targetClient.connect();

  const masterIncludes:string[] = [];

  for(const tableName of tables){
    console.log(`Processing table: ${tableName}`);
    const pkColumns = await getPrimaryKeyColumns(refClient, tableName);
    if(pkColumns.length===0) continue;

    const columns = await getColumnInfo(refClient, tableName);
    const serialCols = await getSerialColumns(refClient, tableName);

    const refRes = await refClient.query(`SELECT * FROM ${tableName}`);
    const targetRes = await targetClient.query(`SELECT * FROM ${tableName}`);

    const refMap = new Map<string,Row>(refRes.rows.map(r=>[pkToString(r,pkColumns), r]));
    const targetMap = new Map<string,Row>(targetRes.rows.map(r=>[pkToString(r,pkColumns), r]));

    const yamlLines:string[] = [];

    // INSERT
    const missingRows = [...refMap.entries()].filter(([k])=>!targetMap.has(k)).map(([_,r])=>r);
    if(missingRows.length>0) yamlLines.push(...generateBatchedInsertYAML(tableName, missingRows, columns));

    // UPDATE
    const updateRows:UpdateRow[] = [];
    for(const [key, targetRow] of targetMap.entries()){
      if(refMap.has(key)){
        const refRow = refMap.get(key)!;
        if(columns.some(col=>!valuesAreEqual(refRow[col.name],targetRow[col.name],col.data_type))){
          updateRows.push({refRow,targetRow});
        }
      }
    }
    if(updateRows.length>0) yamlLines.push(...generateBatchedUpdateYAML(tableName, updateRows, pkColumns, columns));

    // DELETE
    const deleteRows = [...targetMap.entries()].filter(([k])=>!refMap.has(k)).map(([_,r])=>r);
    deleteRows.forEach(r=>yamlLines.push(generateDeleteYAML(tableName,r,pkColumns)));

    // SEQUENCES
    serialCols.forEach(col=>{
      const seqMatch = col.column_default.match(/nextval\('(.+?)'::regclass\)/);
      if(seqMatch){
        const seqName = seqMatch[1];
        const maxValue = refRes.rows.length>0 ? Math.max(...refRes.rows.map(r=>r[col.column_name])) : 0;
        yamlLines.push(generateSequenceYAML(tableName, col.column_name, seqName, maxValue));
      }
    });

    if(yamlLines.length>0){
      const fileName = `${tableName}-data-diff.yaml`;
      fs.writeFileSync(fileName, `databaseChangeLog:\n${yamlLines.join('\n')}`, 'utf8');
      console.log(`Generated YAML for table: ${fileName}`);
      masterIncludes.push(fileName);
    }
  }

  // Only generate master changelog if there are any table diffs
  if(masterIncludes.length > 0){
    const masterYamlLines:string[] = [
      'databaseChangeLog:',
      '  - changeSet:',
      `      id: disable-fk-${Date.now()}`,
      '      author: auto-generated',
      '      changes:',
      '        - sql:',
      '            sql: SET session_replication_role = replica;'
    ];

    masterIncludes.forEach(f=>{
      masterYamlLines.push(`  - include:\n      file: ${f}`);
    });

    masterYamlLines.push(
      '  - changeSet:',
      `      id: enable-fk-${Date.now()}`,
      '      author: auto-generated',
      '      changes:',
      '        - sql:',
      '            sql: SET session_replication_role = DEFAULT;'
    );

    fs.writeFileSync('master-changelog.yaml', masterYamlLines.join('\n'),'utf8');
    console.log('Generated master-changelog.yaml');
  } else {
    console.log('No differences found; master-changelog.yaml not created.');
  }

  await refClient.end();
  await targetClient.end();
}

// Run
generateDiffPerTable();