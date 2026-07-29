import { Codec } from "@nomadshiba/codec";
import { equals } from "@std/bytes";
import { chainStore } from "~/chain/ChainStorage.ts";
import { SpenderIndexer } from "~/chain/SpenderIndexer.ts";
import { StoredPrevOutTxId } from "~/codec/stored/StoredPrevOutTxId.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { WireTxs } from "~/codec/wire/WireTxs.ts";
import { COINBASE_TXID } from "~/constants.ts";
import { PARALLELISM_THREADS } from "~/env.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { MessagePortLike } from "~/libs/message/mod.ts";

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

	const target = chainStore.stores.block.size() - 1;
	p2pPort.addEventListener("message", (event) => p2pMessageQueue.enqueue(event.data));
	p2pPort.postMessage({ type: "seek", data: target });
	p2pPort.postMessage({ type: "start" });
	spenderIndexer.catchUp(target);
}

async function tick() {
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
		let height = chainStore.stores.header.size();
		for (const header of headers) {
			height = chainStore.stores.header.push(header);
			const hash = header.hash();
			chainStore.stores.blockhash.put(hash, height);
		}
		chainStore.atomic.pin();
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

	// TODO: this is a temp sequential single worker impl to figure out the steps for the parallel.
	let txStoreOffset = chainStore.stores.tx.size();

	for (const chunk of batch) {
		let offset = 0;
		while (offset < chunk.length) {
			const [block, size] = WireTxs.decode(chunk.subarray(offset));
			offset += size;

			chainStore.stores.block.push(chainStore.stores.txid.size());

			// TODO: we should probably directly encode into the mmap
			txStoreOffset += chainStore.stores.tx.writeInto(txStoreOffset, WireTxs.counter.encode(block.length));

			for (const tx of block) {
				const txIdPointer = chainStore.stores.txid.put(tx.txId, txStoreOffset);
				const storedTx: Codec.InferInput<typeof StoredTx> = {
					locktime: tx.locktime,
					version: tx.version,
					inputs: tx.inputs.map((input, index): Codec.InferInput<typeof StoredTxInput> => {
						let prevOutTxId: Codec.InferInput<typeof StoredPrevOutTxId>;
						if (equals(input.prevOut.txId, COINBASE_TXID)) {
							prevOutTxId = null;
						} else {
							const pointer = chainStore.stores.txid.getPointer(input.prevOut.txId);
							if (pointer === undefined) {
								throw new Error(""); // TODO: message
							}
							chainStore.stores.spender.put({ tx: pointer, output: input.prevOut.output }, txIdPointer);
							prevOutTxId = pointer;
						}

						return {
							prevOut: { txId: prevOutTxId, output: input.prevOut.output },
							scriptSig: input.scriptSig,
							sequence: input.sequence,
							witness: tx.witness[index] ?? [],
						};
					}),
					outputs: tx.outputs.map((output): Codec.InferInput<typeof StoredTxOutput> => {
						let pubkeyResult = chainStore.stores.pubkey.getValueAndPointer(output.scriptPubKey);
						if (!pubkeyResult) {
							const pubKeyPointer = chainStore.stores.pubkey.put(output.scriptPubKey, txIdPointer);
							pubkeyResult = [txIdPointer, pubKeyPointer];
							return {
								value: Number(output.value),
								previousOutputTx: null,
								scriptPubKey: pubKeyPointer,
							};
						}
						const [previousTxIdPointer, pubKeyPointer] = pubkeyResult;
						chainStore.stores.pubkey.setValue(pubKeyPointer, txIdPointer);
						return {
							value: Number(output.value),
							previousOutputTx: previousTxIdPointer,
							scriptPubKey: pubKeyPointer,
						};
					}),
				};

				// TODO: we should probably directly encode into the mmap
				// (probably can even do it for every part without allocating arrays and objects)
				txStoreOffset += chainStore.stores.tx.writeInto(txStoreOffset, StoredTx.encode(storedTx));
			}
		}
	}

	chainStore.atomic.pin();
}

self.addEventListener("message", async (event) => {
	const port = event.ports[0]!;
	await prepare(port);
	port.start();
	while (true) {
		try {
			await tick();
		} catch (error) {
			console.error("[chain] tick error:", error);
		}
	}
}, { once: true });
