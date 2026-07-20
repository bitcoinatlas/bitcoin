import { dirname, join } from "@std/path";
import { parseArgs } from "@std/cli";

export const DEV = !Deno.build.standalone;
export const ARGS = parseArgs(Deno.args, { boolean: ["background"] });
export const BASE_DIR = Deno.build.standalone ? dirname(Deno.execPath()) : Deno.cwd();
export const BASE_DATA_DIR = join(BASE_DIR, "data");
await Deno.mkdir(BASE_DATA_DIR, { recursive: true });

// TODO: Later keep this inside the workers, tell it to workers while initilizing them.
// this way we can terminate workers and restart them when settings changed.
export const PARALLELISM_THREADS = navigator.hardwareConcurrency;
