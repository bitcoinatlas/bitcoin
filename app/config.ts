import { dirname, join } from "@std/path";

export const DEV = !Deno.build.standalone;
export const BASE_DIR = Deno.build.standalone ? dirname(Deno.execPath()) : Deno.cwd();
export const BASE_DATA_DIR = join(BASE_DIR, "data");
await Deno.mkdir(BASE_DATA_DIR, { recursive: true });
