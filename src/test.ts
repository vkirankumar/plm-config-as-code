import { log } from "console";
import { configDotenv } from 'dotenv';
import { lint } from 'yaml-lint';
import fs, { readdirSync } from "fs";

configDotenv();

const init = async () => {
    log("Hello Liquibase!!!");
    try {
        let str = process.env.DB_USERNAME;
        log("Username = " + str?.substring(0, str.length - 1));
        str = process.env.DB_PASSWORD;
        log("Password = " + str?.substring(0, str.length - 1));
        str = process.env.PORT;
        log("Post = " + str?.substring(0, str.length - 1));
        str = process.env.DB_HOST;
        log("Host = " + str?.substring(0, str.length - 1));
        str = process.env.DB_REFERENCE;
        log("Ref DB = " + str?.substring(0, str.length - 1));
        str = process.env.DB_TARGET;
        log("Target DB = " + str?.substring(0, str.length - 1));
        fs.readFile('./db/masetr/changelog-schema-data.yaml', (err, data) => {
            if (!err) {
                log(data);
                lint(data.toString()).then(result => log(result)).catch(error => log(error));
            } else {
                log(err);
            }           
        });
        
    } catch (error) {
        log("Failed with error " + error);
    }
}

init();