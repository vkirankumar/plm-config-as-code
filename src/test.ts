import { log } from "console";
import { configDotenv } from 'dotenv';

configDotenv();

const init = async () => {
    log("Hello Liquibase!!!");
    try {
        const host = process.env.DB_HOST;
        log(host?.substring(2, 14));
    } catch (error) {
        log("Failed with error " + error);
    }
}

init();