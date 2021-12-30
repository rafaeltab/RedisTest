import { readFileSync, writeFileSync } from "fs";
import * as semver from "semver";

const res = readFileSync("package.json", {
    encoding: "utf8"
});

const packagejson = JSON.parse(res);
const oldVersion = packagejson.version;
const versionParsed = semver.parse(oldVersion, {
    loose: false,
    includePrerelease: true
});

const newVersionFormatted = `${versionParsed.major}.${versionParsed.minor}.${versionParsed.patch}+${parseInt(versionParsed.build[0])+1}`;

console.log(`Increasing version from ${oldVersion} to ${newVersionFormatted}`);

packagejson.version = newVersionFormatted;
writeFileSync("package.json", JSON.stringify(packagejson, null, 4));