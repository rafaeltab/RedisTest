import * as fs from "./libs/fs";
import * as variables from "../VARIABLES.json";
import { sync as globSync } from "glob";

(async () => {
    console.log("Retrieving variables");
    const variableNames = Object.keys(variables);
    const variableRegex: { [Property in keyof typeof variables]: RegExp } = variableNames.map(x => {
        const g = {}
        g[x] = new RegExp(`\\$${x}\\$`, "g");
        return g;
    }).reduce((prev, curr) => {
        return Object.assign(prev, curr);
    }) as { [Property in keyof typeof variables]: RegExp };

    console.log("Getting a list of all files in this directory");
    const allFiles = globSync("**/*.*");

    const excludeRegexes = [
        /node_modules/,
        /dist/,
        /index\.ts/
    ]

    console.log("Removing some ignored files");
    const theFiles = allFiles.filter(x => {
        return excludeRegexes.filter(y => y.test(x)).length == 0
    });

    console.log("Checking each file to see if it needs replacing of variables");
    const filesWithReplace = []

    for (const file of theFiles) {
        const data = await fs.readFileAsync(file, {
            encoding: "utf8"
        });

        for (const varName of variableNames) {
            if (data.includes(`$${varName}$`)) {
                filesWithReplace.push(file);
                console.log(`File needs replacing ${file}`);
                break;
            }
        }
    }

    console.log("\nReplacing variables");
    for (const file of filesWithReplace) {
        console.log(`Replacing file ${file}`);
        let data = await fs.readFileAsync(file, {
            encoding: "utf8"
        });

        for (const varName of variableNames) {
            data = data.replace(variableRegex[varName], variables[varName]);
        }

        fs.writeFileSync(file, data);
    }

})();
