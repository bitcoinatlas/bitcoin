import { addPeer, addPeersFromDNS, availablePeers, expireFailed, peers } from "~/peers.ts";
import { PeerChain } from "./lib/chain/PeerChain.ts";

const MAGIC = new Uint8Array([0xf9, 0xbe, 0xb4, 0xd9]); // mainnet
const PORT = 8333;
const MAX_PEERS = 8;
const FAILED_RETRY_MS = 5 * 60 * 1000;

const DNS_SEEDS = [
	"dnsseed.bitcoin.dashjr.org",
];

while (true) {
	try {
		await tick();
	} catch (error) {
		console.error("[main] Error occurred:", error);
	}
}

async function tick() {
	await maintain();
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

	// fall back to DNS seeds
	for (const seed of DNS_SEEDS) {
		const added = await addPeersFromDNS(seed, PORT);
		if (added > 0) break;
	}
}
