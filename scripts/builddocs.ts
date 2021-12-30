import { rmSync } from "fs";
import { Application, TSConfigReader, TypeDocReader} from "typedoc";
import typedocOptions from "../typedoc.json";

(async () => {
    console.log("Starting docs build...\n")

    // remove current build
    rmSync("./docs", {
        recursive: true,
        force: true
    });

    const app = new Application();

    app.options.addReader(new TSConfigReader());
    app.options.addReader(new TypeDocReader());

    app.bootstrap(typedocOptions);

    const project = app.convert();

    if (project && typedocOptions["out"] != undefined) {
        await app.generateDocs(project, typedocOptions["out"]);
    }
})();

