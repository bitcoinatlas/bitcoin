import { Codec } from "@nomadshiba/codec";
import { equals } from "@std/bytes";
import { delay } from "@std/async";
import { chainStore } from "~/chain/ChainStorage.ts";
import { StoredPrevOutTxId } from "~/codec/stored/StoredPrevOutTxId.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { WireTxs } from "~/codec/wire/WireTxs.ts";
import { COINBASE_TXID } from "~/constants.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { MessagePortLike } from "~/libs/message/mod.ts";

// Stores this worker OWNS: it is the only writer, and it pins ONLY these. The
// header / blockhash stores belong to the p2p worker's domain — this worker
// never writes them (p2p persists headers straight to mmap; we only consume the
// block bodies it forwards). See Atomic.pin(names) for why the split matters.
const CHAIN_STORES = ["block", "tx", "txid", "pubkey", "spender"] as const;

const p2pMessageQueue = new Queue<{ type: string; data: any }>(1000);
const chunkQueue = new Queue<Uint8Array>(256);

let p2pPort!: MessagePortLike;

function prepare(port: MessagePortLike): void {
	p2pPort = port;
	port.addEventListener("message", (event) => p2pMessageQueue.enqueue(event.data));

	// Tell p2p where our block data ends so it downloads from the next height.
	// (Headers are p2p's own domain now — it reads them from mmap, we don't send
	// or receive any header messages here.)
	const target = chainStore.stores.block.size() - 1;
	port.postMessage({ type: "seek", data: target });
	port.postMessage({ type: "start" });
}

async function tick(): Promise<void> {
	const message = p2pMessageQueue.dequeue();
	if (!message) {
		if (chunkQueue.size() > 0) {
			await consumeChunks();
		} else {
			await delay(1); // nothing to do — don't peg the core
		}
		return;
	}

	if (message.type === "blocks") {
		if (!chunkQueue.enqueue(message.data as Uint8Array)) {
			console.error("[chain] chunkQueue overflow — p2p backpressure is not holding");
			Deno.kill(Deno.pid);
		}
		await consumeChunks();
		return;
	}
}

/**
 * Consume exactly one downloaded chunk (many blocks, raw WireTxs back to back),
 * write its txs + indexes, commit, and ack p2p so it can release backpressure.
 *
 * TEMP: single-threaded and sequential — the parallel consume/spender pipeline
 * is deliberately not wired in yet. The goal here is a correct, functional loop
 * on the mmap storage; parallelism (and encoding straight into the mmap) comes
 * back on top of this.
 */
async function consumeChunks(): Promise<void> {
	try {
		const chunk = chunkQueue.dequeue();
		if (!chunk) return;

		// tx is a raw BlobStore. writeInto() fills bytes AHEAD of the cursor
		// without moving it, so we track the offset ourselves and advance the
		// cursor once at the end (see the resize() below). Starting point is the
		// live cursor = end of the last committed round.
		let txStoreOffset = chainStore.stores.tx.size();

		let offset = 0;
		while (offset < chunk.length) {
			const [block, size] = WireTxs.decode(chunk.subarray(offset));
			offset += size;

			// block[height] -> pointer to this block's first txid entry.
			chainStore.stores.block.push(chainStore.stores.txid.size());

			// NOTE (known limitation): writeInto throws if a record straddles a
			// chunkSize (1 GiB) boundary — this stopgap does NOT seal early, so it
			// will throw once tx data first crosses that boundary. Clean straddle
			// handling needs the offset to come from append() (or a reserve pass),
			// which is exactly what the parallel commit-phase rewrite does. Fine
			// for bring-up (first ~1 GiB of tx data).
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
								throw new Error("prevOut references a txid not present in the index");
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

				txStoreOffset += chainStore.stores.tx.writeInto(txStoreOffset, StoredTx.encode(storedTx));
			}
		}

		// Commit the tx blob: advance its cursor past everything we wrote so the
		// bytes become live/readable (the txid pointers we stored point into this
		// range). Then pin OUR domain only — one consistent snapshot per round.
		chainStore.stores.tx.resize(txStoreOffset);
		chainStore.atomic.pin(CHAIN_STORES);

		// Ack so p2p's postedChunks - consumedChunks backpressure can drain.
		p2pPort.postMessage({ type: "consume" });
	} catch (reason) {
		console.error(`[chain] consumeChunks:`, reason);
		Deno.kill(Deno.pid);
	}
}

self.addEventListener("message", (event) => {
	const port = event.ports[0]!;
	prepare(port);
	port.start();

	(async () => {
		while (true) {
			try {
				await tick();
			} catch (error) {
				console.error("[chain] tick error:", error);
			}
		}
	})();
}, { once: true });
