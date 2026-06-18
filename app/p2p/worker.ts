import { PeerChain } from "~/chain/PeerChain.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { addPeer } from "~/p2p/peers.ts";
import { syncHeader, verifyAndPushHeaders } from "~/p2p/utils/headers.ts";

/*

start
get the loaded headers to sync with the mainthread
verify and push them to your localchain

send back `headers` messages to send header updates such:
- new headers
- or reorg point

just makes sure each time you reorg
or push to your headers also let the mainthread know

time to time the mainthread will send you things like target block height

this is for download block txs (aka block bodies).

you should sync and send txs to them until that point,
of course this point can change over time

if you ever send reorg assume target height is also resetted so expect it back.

---

so basically at first on start:
- you get the initial header state from the mainthread

then:
- you keep your own local copy of the headers
- and push the new headers to the mainthread
- push reorgs
- and also based on the height mainthread wants you send blocks(txs) in chunks

you just keep doing that.

make sure to do message queues, and handle them in order.

---

ALSO, main thread can give you a list of blacklisted blocks in the future. or another verification worker

*/

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
