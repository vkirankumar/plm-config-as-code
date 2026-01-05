import { log } from "console";

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