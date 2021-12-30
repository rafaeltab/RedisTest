import { spawn } from "child_process";

export function exec(command: string, args: readonly string[] = [], callback?: (code: number | null) => void) {
    const spRet = spawn(command, args, {
        windowsHide: true,
        stdio: "inherit"
    });
    spRet.on('close', (code) => {
        if (callback) callback(code);
    })
}

export function execAsync(command: string, args: readonly string[] = []): Promise<number | null> {
    return new Promise((resolve) => {
        exec(command, args, (code) => {
            resolve(code);
        });
    });
}