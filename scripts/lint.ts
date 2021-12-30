import { exec } from "./libs/execute";

const args = ["eslint", "."]

if (process.argv.includes("--fix")) { 
    args.push("--fix");
}

exec(/^win/.test(process.platform) ? 'npx.cmd' : 'npx', args);