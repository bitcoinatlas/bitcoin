import { Router } from "~/app/libs/routing/Router.ts";
import { SCHEMA } from "~/app/routes.ts";

export const endpointRouter = new Router({ schema: SCHEMA });
