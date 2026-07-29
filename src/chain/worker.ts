import { chainStorage } from "~/chain/ChainStorage.ts";
import { SpenderIndexer } from "~/chain/SpenderIndexer.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { PARALLELISM_THREADS } from "~/env.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { MessagePortLike } from "~/libs/message/mod.ts";

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
		if (chunkQueue.size() > 0) await consumeChunks();
		return;
	}

	if (message.type === "blocks") {
		if (!chunkQueue.enqueue(message.data as Uint8Array)) {
			console.error("[chain] chunkQueue overflow — p2p backpressure is not holding");
			Deno.kill(Deno.pid);
		}
		if (chunkQueue.size() >= consumers.length) await consumeChunks();
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

async function consumeChunks() {
	const batchSize = Math.min(consumers.length, chunkQueue.size());
	if (batchSize === 0) return;
	const batch: Uint8Array[] = new Array(batchSize);
	for (let i = 0; i < batchSize; i++) batch[i] = chunkQueue.dequeue()!;
}
