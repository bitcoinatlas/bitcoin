import { endpointRouter } from "~/api/router.ts";
import appHtml from "./ui/app.html" with { type: "text" };
import appJs from "./ui/dist/app.js" with { type: "text" };

const app = appHtml.replace("<!-- inject js -->", () => `<script type="module">${appJs}</script>`);

import "~/api/handlers/block.ts";

export function serve(port: number) {
	Deno.serve({ port }, async (request, _info) => {
		const url = new URL(request.url);
		const { pathname } = url;

		if (request.method === "GET" && pathname === "/") {
			return new Response(app, { headers: { "Content-Type": "text/html" } });
		}

		return await endpointRouter.resolveRequest(request);
	});
}
