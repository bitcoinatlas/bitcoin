import { Router } from "~/libs/routing/mod.ts";
import { SCHEMA } from "~/routes.ts";

export const endpointRouter = new Router({ schema: SCHEMA });
