import { getBlockHeaderByHeight, getChainTip } from "~/chain.ts";
import { join } from "@std/path";

const PORT = 3000;
const FRONTEND_DIR = new URL("./frontend/", import.meta.url).pathname;

// Bundle frontend/app.ts into JS on startup
async function bundleFrontend(): Promise<string> {
	const entryPoint = join(FRONTEND_DIR, "app.ts");
	const cmd = new Deno.Command("deno", {
		args: ["bundle", entryPoint],
		stdout: "piped",
		stderr: "piped",
	});
	const { code, stdout, stderr } = await cmd.output();
	if (code !== 0) {
		throw new Error(`deno bundle failed:\n${new TextDecoder().decode(stderr)}`);
	}
	return new TextDecoder().decode(stdout);
}

const bundledJs = await bundleFrontend();
console.log(`[server] frontend bundled (${bundledJs.length} bytes)`);

const HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bitcoin Explorer</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: #0d0d0d; color: #e0e0e0; font-family: monospace; font-size: 14px; }
header { background: #111; border-bottom: 1px solid #222; padding: 1rem 2rem; display: flex; align-items: center; gap: 1rem; }
header h1 { font-size: 1.1rem; color: #f7931a; letter-spacing: 0.05em; }
#status { font-size: 0.85rem; color: #888; }
#blocks { max-width: 860px; margin: 1.5rem auto; padding: 0 1rem; display: flex; flex-direction: column; gap: 0.5rem; }
.block { background: #141414; border: 1px solid #222; border-radius: 4px; padding: 0.75rem 1rem; display: grid; grid-template-columns: 5rem 1fr 1fr 1fr; align-items: center; gap: 0.5rem; transition: border-color 0.15s; }
.block:hover { border-color: #f7931a44; }
.block-height { color: #f7931a; font-weight: bold; font-size: 1rem; }
.block-meta { display: flex; align-items: center; gap: 0.4rem; flex-wrap: wrap; }
.label { color: #555; font-size: 0.8rem; }
.hash { color: #7eb8f7; font-size: 0.8rem; cursor: default; }
@media (max-width: 600px) {
  .block { grid-template-columns: 1fr; }
}
</style>
</head>
<body>
<header>
  <h1>⛏ Bitcoin Explorer</h1>
  <span id="status">Loading…</span>
</header>
<div id="blocks"></div>
<script>${bundledJs}</script>
</body>
</html>`;

// ---- API helpers ----

function jsonResponse(data: unknown, status = 200): Response {
	return new Response(JSON.stringify(data), {
		status,
		headers: { "Content-Type": "application/json" },
	});
}

function toHex(bytes: Uint8Array): string {
	// Bitcoin displays hashes in reversed byte order
	return [...bytes].reverse().map((b) => b.toString(16).padStart(2, "0")).join("");
}

// ---- Request handler ----

async function handle(req: Request): Promise<Response> {
	const url = new URL(req.url);

	// Serve frontend
	if (url.pathname === "/") {
		return new Response(HTML, { headers: { "Content-Type": "text/html; charset=utf-8" } });
	}

	// GET /api/tip
	if (url.pathname === "/api/tip") {
		const tip = await getChainTip();
		if (!tip) return jsonResponse({ error: "no chain" }, 503);
		return jsonResponse({ height: tip.height, hash: toHex(tip.hash) });
	}

	// GET /api/headers?from=<height>&limit=<n>
	if (url.pathname === "/api/headers") {
		const from = parseInt(url.searchParams.get("from") ?? "0");
		const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "20"), 100);

		if (isNaN(from) || isNaN(limit)) {
			return jsonResponse({ error: "bad params" }, 400);
		}

		const tip = await getChainTip();
		const tipHeight = tip?.height ?? 0;

		// Return headers from `from` downward (newest first scroll)
		const headers = [];
		for (let h = from; h > from - limit && h >= 0; h--) {
			const header = await getBlockHeaderByHeight(h);
			if (!header) break;
			headers.push({
				height: h,
				hash: toHex(header.hash),
				prevHash: toHex(header.prevHash),
				timestamp: header.timestamp,
				bits: header.bits,
				nonce: header.nonce,
				version: header.version,
			});
		}

		return jsonResponse({ headers, tip: tipHeight });
	}

	return new Response("not found", { status: 404 });
}

console.log(`[server] listening on http://localhost:${PORT}`);
Deno.serve({ port: PORT }, handle);
