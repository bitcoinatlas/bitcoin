import { chainStorage } from "~/chain/ChainStorage.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { PARALLELISM_THREADS } from "~/env.ts";
import { SpenderIndexer } from "~/chain/SpenderIndexer.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { MessagePortLike } from "~/libs/message/mod.ts";
import { delay } from "@std/async";
import { EncodedBlock } from "~/chain/consume.worker.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";

/** consume.worker `init` output: unknown scriptPubKeys, pre-hashed + pre-encoded. */
type InitResult = {
	/** hash of each unknown pubkey, packed 32 bytes each (for cross-worker dedup). */
	hashes: Uint8Array;
	/** StoredScriptPubKey bytes of each unknown pubkey, back-to-back. */
	encoded: Uint8Array;
	/** encoded length of each unknown pubkey; slices `encoded`. */
	lengths: Uint32Array;
};

self.addEventListener("message", async (event) => {
	const port = event.ports[0]!;
	await prepare(port);
	port.start();
	while (true) {
		try {
			await tick(port);
		} catch (error) {
			console.error("[chain] tick error:", error);
		}
	}
}, { once: true });

const p2pMessageQueue = new Queue<{ type: string; data: any }>(1000);
const consumers = new Array(PARALLELISM_THREADS);
const chunkQueue = new Queue<Uint8Array>(256);
const spenderIndexer = new SpenderIndexer();

async function prepare(p2pPort: MessagePortLike) {
	for (let i = 0; i < consumers.length; i++) {
		const worker = new Worker(new URL("./consume.worker.ts", import.meta.url), { type: "module", name: `consumer-${i}` });
		worker.addEventListener("error", (event) => {
			console.error(`[chain] consumer-${i} uncaught:`, event.message, event.filename, event.lineno);
			Deno.kill(Deno.pid);
		});
		consumers[i] = worker;
	}

	const target = chainStorage.stores.block.size() - 1;
	p2pPort.addEventListener("message", (event) => p2pMessageQueue.enqueue(event.data));
	p2pPort.postMessage({ type: "seek", data: target });
	p2pPort.postMessage({ type: "start" });
	spenderIndexer.catchUp(target);
}

async function tick(p2pPort: MessagePortLike) {
	const message = p2pMessageQueue.dequeue();
	if (!message) {
		// Nothing pending. If chunks are waiting but we never reached a full
		// batch (tail of IBD, or p2p idle at the tip), flush the partial now so
		// it doesn't stall. During fast IBD the queue reaches batchSize before
		// it ever empties, so this only fires when p2p genuinely has nothing.
		if (chunkQueue.size() > 0) {
			await consumeBatch(batchChunkQueue());
		}
		return;
	}

	if (message.type === "blocks") {
		if (!chunkQueue.enqueue(message.data as Uint8Array)) {
			console.error("[chain] chunkQueue overflow — p2p backpressure is not holding");
			Deno.kill(Deno.pid);
		}
		if (chunkQueue.size() >= consumers.length) await consumeBatch(batchChunkQueue());
		return;
	}

	if (message.type === "headers") {
		const [headers] = WireBlockHeaders.decode(message.data);
		await handleHeadersMessage(headers);
		return;
	}

	if (message.type === "reorg") {
		handleReorgMessage(message.data as number);
		return;
	}
}

function handleReorgMessage(_keepHeight: number): void {
	throw new Error("Not Implemented");
}

async function handleHeadersMessage(headers: WireBlockHeaders) {
	try {
		let height = chainStorage.stores.header.size();
		for (const header of headers) {
			height = chainStorage.stores.header.push(header);
			const hash = header.hash();
			chainStorage.stores.blockhash.set(hash, height);
		}
		chainStorage.atomic.pin();
		return { height };
	} catch (reason) {
		console.error("Failed to append block header:", reason);
		Deno.kill(Deno.pid);
	}
}

function batchChunkQueue(): Uint8Array[] {
	const n = Math.min(consumers.length, chunkQueue.size());
	const batch: Uint8Array[] = new Array(n);
	for (let i = 0; i < n; i++) batch[i] = chunkQueue.dequeue()!;
	return batch;
}

async function consumeBatch(batch: Uint8Array[]) {
}
