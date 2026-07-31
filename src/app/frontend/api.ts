import { RouterClient } from "~/app/libs/routing/RouterClient.ts";
import { SCHEMA } from "~/app/routes.ts";

export const api = RouterClient.create<typeof SCHEMA>({
	baseUrl: new URL("/", location.href),
	schema: SCHEMA,
	fetch: (...args) => fetch(...args),
});
