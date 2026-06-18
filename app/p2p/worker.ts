import { PeerChain } from "~/chain/PeerChain.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { addPeer } from "~/p2p/peers.ts";
import { syncHeader, verifyAndPushHeaders } from "~/p2p/utils/headers.ts";

await new Promise<void>((resolve) => {
	const controller = new AbortController();
	self.addEventListener("message", (event) => {
		const message = event.data;
		if (message.name !== "start") return;
		controller.abort();
		const [headers] = WireBlockHeaders.decode(message.data);
		verifyAndPushHeaders(localChain, headers);
		resolve();
	}, { signal: controller.signal });
});

const MAGIC = new Uint8Array([0xf9, 0xbe, 0xb4, 0xd9]); // mainnet
const P2P_PORT = 8333;
/* const MAX_PEERS = 8;
const FAILED_RETRY_MS = 5 * 60 * 1000;
const DNS_SEEDS = [
	"dnsseed.bitcoin.dashjr.org",
]; */

const peer = await addPeer("192.168.8.10", P2P_PORT, MAGIC);
if (!peer) {
	console.error("Not handled disconnected peer yet");
	Deno.exit(1);
}

const localChain = new PeerChain();

while (!await syncHeader(peer, localChain)) {
	console.log("syncing headers");
}

const headers = WireBlockHeaders.encode(localChain.values().map(({ header }) => header).toArray());
self.postMessage({ name: "headers", data: headers }, [headers.buffer]);
