import { log } from "console";
import express from "express";

const app = express();
const port: number = 3000;

app.listen(port);

log("Node application started at port " + port);

app.get("/health", (req, res) => {
    res.send("OK");
    res.status(200);
});

app.get("/", (req, res) => {
    res.setHeader('Content-Type', 'text/html');
    res.send(`
        <html>
            <body>
                <h1>New Node app with ArgoCD!!</h1>
            </body>
        </html>`);
});