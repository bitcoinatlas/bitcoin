import { RouterClient } from "~/app/libs/routing/RouterClient.ts";
import { ROUTES_SCHEMA } from "~/app/routes.ts";

export const api = RouterClient.create<typeof ROUTES_SCHEMA>({
	baseUrl: new URL("/", location.href),
	schema: ROUTES_SCHEMA,
	fetch,
});
