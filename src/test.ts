import { log } from "console";
import { configDotenv } from 'dotenv';

configDotenv();

const init = async () => {
    log("Hello Liquibase!!!");
    try {
        let str = process.env.DB_USERNAME;
        log("Username = " + str?.substring(0, str.length));
        str = process.env.DB_PASSWORD;
        log("Password = " + str?.substring(0, str.length));
        str = process.env.PORT;
        log("Post = " + str?.substring(0, str.length));
        str = process.env.DB_HOST;
        log("Host = " + str?.substring(0, str.length));
        str = process.env.DB_REFERENCE;
        log("Ref DB = " + str?.substring(0, str.length));
        str = process.env.DB_TARGET;
        log("Target DB = " + str?.substring(0, str.length));
    } catch (error) {
        log("Failed with error " + error);
    }
}

init();