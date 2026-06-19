import { EndpointRouter } from "~/app/backend/utils/EndpointRouter.ts";
import { ENDPOINT_SCHEMA } from "~/app/backend/schema.ts";

export const endpointRouter = new EndpointRouter({
	schema: ENDPOINT_SCHEMA,
});
