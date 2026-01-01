import { error, log } from 'console';
import {
    Liquibase,
    POSTGRESQL_DEFAULT_CONFIG
} from 'liquibase';
import { readdirSync } from 'fs';

// const args = process.argv.slice(2);
const output = process.env.GITHUB_OUTPUT;

const init = async () => {
    const changeDirectoryName = getDirectoryName();
    if (!changeDirectoryName) {
        error("No change log directory or master changelog file found!!! ");
        return;
    }
    log("Starting merge!!");
    log(`Applying changes from directory '${changeDirectoryName}'`);
    try {
        await update(changeDirectoryName);
        log("Merge Completed!!");
    } catch (error) {
        log("Failed with error " + error);
    }
}

const update = async (changeDirectoryName: string) => {
    const config = {
        ...POSTGRESQL_DEFAULT_CONFIG,
        password: 'npg_cweS1VpKl0JL',
        username: 'neondb_owner',
        url: 'jdbc:postgresql://ep-summer-hill-ad1oha2r-pooler.c-2.us-east-1.aws.neon.tech:5432/dev',
        changeLogFile: `./db/data-diffs/${changeDirectoryName}/master-changelog.yaml`,
    };
    const liquibase: Liquibase = new Liquibase(config);
    await liquibase.update({});
}

const getDirectoryName = () => {
    const directories: string[] = readdirSync("./db/data-diffs");
    let isMasterChangeLogFound: boolean = false;
    if (directories[0]) {
         isMasterChangeLogFound = readdirSync(`./db/data-diffs/${directories[0]}`)
         .includes('master-changelog.yaml');
    }
    return isMasterChangeLogFound ? directories[0] : null;
}

init();