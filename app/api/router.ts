import { EndpointRouter } from "~/api/utils/EndpointRouter.ts";
import { ENDPOINT_SCHEMA } from "~/api/schema.ts";

export const endpointRouter = new EndpointRouter({
	schema: ENDPOINT_SCHEMA,
});
