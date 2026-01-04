import { error, log } from "console";
import { Liquibase, POSTGRESQL_DEFAULT_CONFIG } from "liquibase";
import fs, { readdirSync } from "fs";
import { configDotenv } from 'dotenv';

// const args = process.argv.slice(2);
const output = process.env.GITHUB_OUTPUT;
configDotenv();

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
};

const update = async (changeDirectoryName: string) => {
    try {
        const config = {
            ...POSTGRESQL_DEFAULT_CONFIG,
            password: process.env.DB_PASSWORD ?? '',
            username: process.env.DB_USERNAME ?? '',
            url: `jdbc:postgresql://${process.env.DB_HOST}:${process.env.PORT}/${process.env.DB_TARGET}`,
            changeLogFile: `./db/data-diffs/${changeDirectoryName}/master-changelog.yaml`,
        };
        const liquibase: Liquibase = new Liquibase(config);
        await liquibase.update({});
    } catch (err) {
        error("Failed with error " + err);
    }
};

const getDirectoryName = () => {
  if (fs.existsSync("./db/diff")) {
    const directories: string[] = readdirSync("./db/diff");
    let isMasterChangeLogFound: boolean = false;

    directories.sort((a, b) => new Date(a).getTime() - new Date(b).getTime());
   
    if (directories.length) {

      isMasterChangeLogFound = readdirSync(
        `./db/diff/${directories[directories.length - 1]}/data`
      ).includes("master-changelog.yaml");
    }
    return isMasterChangeLogFound ? directories[directories.length - 1] : null;
  }
  return null;
};

init();
