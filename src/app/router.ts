import { Router } from "~/app/libs/routing/Router.ts";
import { ROUTES_SCHEMA } from "~/app/routes.ts";

export const endpointRouter = new Router({ schema: ROUTES_SCHEMA });
