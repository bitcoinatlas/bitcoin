import { EndpointRouter } from "~/server/utils/EndpointRouter.ts";
import { ENDPOINT_SCHEMA } from "~/server/schema.ts";

export const endpointRouter = new EndpointRouter({
	schema: ENDPOINT_SCHEMA,
});
