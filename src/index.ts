import { log } from 'console';
import {
    Liquibase,
    POSTGRESQL_DEFAULT_CONFIG
} from 'liquibase';
import fs from 'fs';
import { configDotenv } from 'dotenv';

enum LQ_OPS {
    SCHEMA_DIFF = 'schema-diff',
    DB_SNAPSHOT = 'snapshot',
}

const args = process.argv.slice(2);
configDotenv();
const output = process.env.GITHUB_OUTPUT;

// const config_dev = {
//     ...POSTGRESQL_DEFAULT_CONFIG,
//     password: 'postgres',
//     url: 'jdbc:postgresql://localhost:4001/postgres',
// };

// const config_prod = {
//     ...POSTGRESQL_DEFAULT_CONFIG,
//     changeLogFile: './changelog-data-1.xml',
//     password: 'postgres',
//     url: 'jdbc:postgresql://localhost:4001/prod',
// };

/**
 * TODO:
 * On PR action, get DB snapshot and push into changelog folder in the PR branch.
 * Update merge action to pick the most recent diff folder or bring from the PR description. 
 */

const diffTypes: string = "table,column,primaryKey,index,foreignKey,uniqueConstraint";
// const referenceParam: string = ' --referenceUrl=jdbc:postgresql://localhost:4001/preprod --referenceUsername=postgres referencePassword=postgres';
// const liquibase_dev: Liquibase = new Liquibase(config_dev);
// const liquibase_prod: Liquibase = new Liquibase(config_prod);
const timeStamp: string  = new Date().toISOString().replace(/[:.&]/g, "-");

const init = async () => {
    log("Hello Liquibase!!!");
    try {
        switch(args[0]) {
            case LQ_OPS.DB_SNAPSHOT:
                    generateChangeLog();
                break;
            case LQ_OPS.SCHEMA_DIFF:
                    schemaDiffChangeLog();
                break;
            default:
                log("Completed!!");
        }
        // generateChangeLog();
        // diff();
        // await update();
        // await diffChangeLog();
    } catch (error) {
        log("Failed with error " + error);
    }
}

const generateChangeLog = (): void => {
    const config_dev = {
        ...POSTGRESQL_DEFAULT_CONFIG,
        password: 'postgres',
        url: 'jdbc:postgresql://localhost:4001/postgres',
    };
    const liquibase: Liquibase = new Liquibase(config_dev);
    liquibase.generateChangeLog({diffTypes,
             changelogFile: `./db/master/changelog-master-${timeStamp}.yaml`});
}

// const schema_diff = async () => {
//     const config = {
//         ...POSTGRESQL_DEFAULT_CONFIG,
//         password: 'postgres',
//         url: 'jdbc:postgresql://localhost:4001/prod',
//         referenceUrl: 'jdbc:postgresql://localhost:4001/dev',
//         referenceUsername: 'postgres',
//         referencePassword: 'postgres',
//         schemas: 'public',
//         referenceSchemas: 'public'
//     };
//     const liquibase: Liquibase = new Liquibase(config);
//     const diff: string = await liquibase.diff({diffTypes});
//     log(diff);
// }

const update = (): void => {
    const config = {
        ...POSTGRESQL_DEFAULT_CONFIG,
        password: 'npg_cweS1VpKl0JL',
        username: 'neondb_owner',
        url: 'jdbc:postgresql://ep-summer-hill-ad1oha2r-pooler.c-2.us-east-1.aws.neon.tech:5432/dev',
        // changeLogFile: './db/master/changelog-schema.yaml',
        changeLogFile: './db/data-diffs/2026-01-01T12-18-14-818Z/master-changelog.yaml',
    };
    const liquibase: Liquibase = new Liquibase(config);
    liquibase.update({});
}

// const update = (): void => {
//     const config = {
//         ...POSTGRESQL_DEFAULT_CONFIG,
//         password: 'postgres',
//         url: 'jdbc:postgresql://localhost:4001/prod',
//         changeLogFile: './master-changelog.yaml',
//     };
//     const liquibase: Liquibase = new Liquibase(config);
//     liquibase.update({});
// }

const schemaDiffChangeLog = async () => {
    const fileName = `changelog-diff-${timeStamp}.yaml`;
    const config = {
        ...POSTGRESQL_DEFAULT_CONFIG,
        password: 'npg_cweS1VpKl0JL',
        username: 'neondb_owner',
        url: 'jdbc:postgresql://ep-summer-hill-ad1oha2r-pooler.c-2.us-east-1.aws.neon.tech:5432/neondb',
        referenceUrl: 'jdbc:postgresql://ep-summer-hill-ad1oha2r-pooler.c-2.us-east-1.aws.neon.tech:5432/dev',
        referenceUsername: 'neondb_owner',
        referencePassword: 'npg_cweS1VpKl0JL',
        schemas: 'public',
        referenceSchemas: 'public',
        changeLogFile: `./db/diff/${fileName}`,
    };
    const liquibase: Liquibase = new Liquibase(config);
    await liquibase.diffChangelog({diffTypes});
    if (fs.existsSync(`./db/diff/${fileName}`)) {
        log(`Diff ${fileName} generated successfully!!!`);
        if (output) {
            fs.appendFileSync(output, `diffFileName=${fileName}\n`);
        }
    } else {
        log("Schemas are equal");
    }
}

init();