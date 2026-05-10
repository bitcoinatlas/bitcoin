import { delay } from "@std/async";
import { atomicFlush } from "~/chain.ts";
import { syncHeadersFromPeers } from "~/headers.ts";
import { addPeer, addPeersFromDNS, availablePeers, expireFailed, peers } from "~/peers.ts";
import { serve } from "~/serve.ts";

const MAGIC = new Uint8Array([0xf9, 0xbe, 0xb4, 0xd9]); // mainnet
const P2P_PORT = 8333;
const HTTP_PORT = 3000;
const FLUSH_INTERVAL_MS = 60 * 1000;
const MAX_PEERS = 8;
const FAILED_RETRY_MS = 5 * 60 * 1000;

const DNS_SEEDS = [
	"dnsseed.bitcoin.dashjr.org",
];

serve(HTTP_PORT);

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
