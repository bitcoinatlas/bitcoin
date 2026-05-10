import { addPeer, addPeersFromDNS, availablePeers, expireFailed, peers } from "~/peers.ts";
import { atomicFlush, getBlockHeaderByHeight, getChainTip } from "~/chain.ts";
import { syncHeadersFromPeers } from "~/headers.ts";
import { join } from "@std/path";
import { delay } from "@std/async";

const MAGIC = new Uint8Array([0xf9, 0xbe, 0xb4, 0xd9]); // mainnet
const P2P_PORT = 8333;
const HTTP_PORT = 3000;
const FLUSH_INTERVAL_MS = 60 * 1000;
const MAX_PEERS = 8;
const FAILED_RETRY_MS = 5 * 60 * 1000;

const DNS_SEEDS = [
	"dnsseed.bitcoin.dashjr.org",
];

// ---- Frontend bundle ----

const FRONTEND_DIR = new URL("./frontend/", import.meta.url).pathname;

async function bundleFrontend(): Promise<string> {
	const entry = join(FRONTEND_DIR, "app.ts");
	const { code, stdout, stderr } = await new Deno.Command("deno", {
		args: ["bundle", entry],
		stdout: "piped",
		stderr: "piped",
	}).output();
	if (code !== 0) throw new Error(`deno bundle failed:\n${new TextDecoder().decode(stderr)}`);
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
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body { background: #0a0a0a; color: #d4d4d4; font-family: ui-monospace, "Cascadia Code", "Fira Code", monospace; font-size: 13px; line-height: 1.5; }

header {
	background: #111;
	border-bottom: 1px solid #1e1e1e;
	padding: 0.85rem 1.5rem;
	display: flex;
	align-items: center;
	gap: 1.5rem;
	position: sticky;
	top: 0;
	z-index: 10;
}
header h1 { font-size: 1rem; color: #f7931a; font-weight: 600; letter-spacing: 0.04em; }
#tip { color: #666; font-size: 0.8rem; }
#status { color: #555; font-size: 0.8rem; margin-left: auto; }

#blocks {
	max-width: 900px;
	margin: 1rem auto;
	padding: 0 1rem;
	display: flex;
	flex-direction: column;
	gap: 2px;
}

.block { border-radius: 3px; overflow: hidden; border: 1px solid #1a1a1a; }
.block.open { border-color: #f7931a55; }

.block-header {
	display: flex;
	align-items: center;
	justify-content: space-between;
	padding: 0.6rem 0.9rem;
	cursor: pointer;
	background: #111;
	user-select: none;
	gap: 1rem;
}
.block-header:hover { background: #161616; }
.block.open .block-header { background: #161616; }

.block-left { display: flex; align-items: center; gap: 1rem; }
.height { color: #f7931a; font-weight: 600; min-width: 5rem; }
.time { color: #666; font-size: 0.78rem; }

.block-right { display: flex; align-items: center; gap: 0.75rem; }
.hash-short { color: #5a8fb5; font-size: 0.8rem; }
.chevron { color: #444; font-size: 0.65rem; transition: transform 0.15s; }
.block.open .chevron { transform: rotate(90deg); }

.block-detail {
	background: #0d0d0d;
	border-top: 1px solid #1a1a1a;
	padding: 0.75rem 0.9rem;
}
.block-detail table { width: 100%; border-collapse: collapse; }
.block-detail td { padding: 0.3rem 0.5rem; vertical-align: top; }
.block-detail td.field { color: #555; width: 8rem; white-space: nowrap; }
.block-detail td.value { color: #c9c9c9; word-break: break-all; }
.hash-full { color: #5a8fb5; font-size: 0.78rem; word-break: break-all; }
.dim { color: #444; }
</style>
</head>
<body>
<header>
	<h1>Bitcoin Explorer</h1>
	<span id="tip"></span>
	<span id="status">Loading…</span>
</header>
<div id="blocks"></div>
<script>${bundledJs}</script>
</body>
</html>`;

// ---- API ----

function jsonResponse(data: unknown, status = 200): Response {
	return new Response(JSON.stringify(data), {
		status,
		headers: { "Content-Type": "application/json" },
	});
}

function toHex(bytes: Uint8Array): string {
	return [...bytes].reverse().map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function handleRequest(req: Request): Promise<Response> {
	const { pathname, searchParams } = new URL(req.url);

	if (pathname === "/") {
		return new Response(HTML, { headers: { "Content-Type": "text/html; charset=utf-8" } });
	}

	if (pathname === "/api/tip") {
		const tip = await getChainTip();
		if (!tip) return jsonResponse({ error: "no chain" }, 503);
		return jsonResponse({ height: tip.height, hash: toHex(tip.hash) });
	}

	if (pathname === "/api/headers") {
		const from = parseInt(searchParams.get("from") ?? "0");
		const limit = Math.min(parseInt(searchParams.get("limit") ?? "20"), 100);
		if (isNaN(from) || isNaN(limit)) return jsonResponse({ error: "bad params" }, 400);

		const tip = await getChainTip();
		const tipHeight = tip?.height ?? 0;
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

Deno.serve({ port: HTTP_PORT }, handleRequest);
console.log(`[server] http://localhost:${HTTP_PORT}`);

// ---- P2P loop ----

// Local dev: single peer. For production, swap with maintain().
await addPeer("192.168.1.10", P2P_PORT, MAGIC);
// await maintain();

let lastFlush = Date.now();

while (true) {
	try {
		await delay(0);
		await tick();
	} catch (error) {
		console.error("[main] tick error:", error);
	}
}

async function tick() {
	// await maintain(); // uncomment for production peer management
	await syncHeadersFromPeers();

	if (Date.now() - lastFlush >= FLUSH_INTERVAL_MS) {
		await atomicFlush();
		lastFlush = Date.now();
	}
}

async function maintain() {
	expireFailed(FAILED_RETRY_MS);

	if (peers().length >= MAX_PEERS) return;

	const available = availablePeers();
	const needed = MAX_PEERS - peers().length;
	await Promise.allSettled(
		available.slice(0, needed).map(({ host, port }) => addPeer(host, port, MAGIC)),
	);

	if (peers().length >= MAX_PEERS) return;

	for (const seed of DNS_SEEDS) {
		const added = await addPeersFromDNS(seed, P2P_PORT);
		if (added > 0) break;
	}
}
