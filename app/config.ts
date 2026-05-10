import { join } from "@std/path";

export const BASE_DIR = Deno.build.standalone ? Deno.execPath() : Deno.cwd();
export const BASE_DATA_DIR = join(BASE_DIR, "data");
