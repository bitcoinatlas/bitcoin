import { serveDir, ServeDirOptions } from "@std/http";
import { tryAdoptControl } from "~/libs/storage/control.ts";
import { endpointRouter } from "~/app/router.ts";
import { DEV } from "~/env.ts";
import appHtml from "~/app/frontend/app.html" with { type: "text" };
import appJs from "~/app/frontend/dist/app.js" with { type: "text" };

const PORT = 58333;
const SERVE_DIR_OPTIONS: ServeDirOptions = {
	showIndex: false,
	showDirListing: false,
	showDotfiles: false,
	fsRoot: new URL("./frontend/assets/", import.meta.url).pathname,
	urlRoot: "assets",
};

await import("~/app/backend/handlers/chain.ts");

// main hands us the shared control block; adopt it so store reads in request
// handlers are fenced against the chain/p2p writers.
self.onmessage = (event) => {
	tryAdoptControl(event.data);
};

let appCache: string | undefined;
async function app() {
	if (DEV) {
		const [appHtml, appJs] = await Promise.all([
			Deno.readTextFile(new URL("./frontend/app.html", import.meta.url)),
			Deno.readTextFile(new URL("./frontend/dist/app.js", import.meta.url)),
		]);
		return appHtml.replace("<!-- inject js -->", () => `<script type="module">${appJs}</script>`);
	}
	return appCache ??= appHtml.replace("<!-- inject js -->", () => `<script type="module">${appJs}</script>`);
}

Deno.serve({ port: PORT }, async (request, _info) => {
	const url = new URL(request.url);
	const { pathname } = url;

	if (request.method === "GET" && pathname === "/") {
		return new Response(await app(), { headers: { "Content-Type": "text/html" } });
	}

	if (request.method === "GET" && pathname.startsWith("/assets/")) {
		return serveDir(request, SERVE_DIR_OPTIONS);
	}

	return await endpointRouter.resolveRequest(request);
});

self.postMessage(null);
