import { readFileSync, writeFileSync } from "fs";
import * as semver from "semver";

const res = readFileSync("package.json", {
    encoding: "utf8"
});

const packagejson = JSON.parse(res);
const oldVersion = packagejson.version;
const newVersion = semver.parse(oldVersion, {
    loose: false,
    includePrerelease: true
}).inc("minor");

const newVersionFormatted = `${newVersion.major}.${newVersion.minor}.${newVersion.patch}+${newVersion.build}`;

console.log(`Increasing version from ${oldVersion} to ${newVersionFormatted}`);

packagejson.version = newVersionFormatted;
writeFileSync("package.json", JSON.stringify(packagejson, null, 4));