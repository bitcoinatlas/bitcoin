const json = await Deno.readTextFile("./vendor/rocksdb-js/package.json");
const packageJson = JSON.parse(json);
packageJson.devDependencies = {};
await Deno.writeTextFile("./vendor/rocksdb-js/package.json", JSON.stringify(packageJson));
await Deno.remove("./vendor/rocksdb-js/.git", { recursive: true }).catch(() => {});
