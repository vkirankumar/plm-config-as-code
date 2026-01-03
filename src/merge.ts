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
  const config = {
    ...POSTGRESQL_DEFAULT_CONFIG,
    password: process.env.DB_PASSWORD ?? '',
    username: process.env.DB_USERNAME ?? '',
    url: `jdbc:postgresql://${process.env.DB_HOST}:${process.env.PORT}/${process.env.DB_TARGET}`,
    changeLogFile: `./db/data-diffs/${changeDirectoryName}/master-changelog.yaml`,
  };
  const liquibase: Liquibase = new Liquibase(config);
  await liquibase.update({});
};

const getDirectoryName = () => {
  if (fs.existsSync("./db/data-diffs")) {
    const directories: string[] = readdirSync("./db/data-diffs");
    let isMasterChangeLogFound: boolean = false;
    if (directories[0]) {
      isMasterChangeLogFound = readdirSync(
        `./db/data-diffs/${directories[0]}`
      ).includes("master-changelog.yaml");
    }
    return isMasterChangeLogFound ? directories[0] : null;
  }
  return null;
};

init();
