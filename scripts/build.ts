import { execAsync } from "./libs/execute";
import { rmSync, existsSync } from "fs";

(async () => {
    console.log("Starting build...\n")

    // remove current build
    rmSync("./dist", {
        recursive: true,
        force: true
    });

    // Install node modules if typescript tsc is not present, we kinda need tsc so yeah.
    if (!existsSync("node_modules/typescript/bin/tsc")) {
        await execAsync("npm", ["install"]);
    }

    // execute build
    const code = await execAsync("node", ["node_modules/typescript/bin/tsc"]);

    if (code == 0) {
        console.log("\nBuild completed with success.")
    } else { 
        console.log(`\nBuild failed with code ${code}`)
    }    
})();

