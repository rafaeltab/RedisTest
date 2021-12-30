export * from "fs";
import { Abortable } from "events";
import { PathOrFileDescriptor, readFile } from "fs";

export function readFileAsync(
    path: PathOrFileDescriptor,
    options:
        | ({
            encoding: BufferEncoding;
            flag?: string | undefined;
        } & Abortable)
        | BufferEncoding
): Promise<string> {
    return new Promise((resolve) => {
        readFile(path, options, (err, data) => {
            if (err) throw err;
            resolve(data);
        })
    });
}

