import { Codec } from "@nomadshiba/codec";
import { equals } from "@std/bytes";
import { delay } from "@std/async";
import { chainStore } from "~/chain/ChainStore.ts";
import { BIP30_EXCEPTION_BLOCKS, isBip30Exception } from "~/chain/bips/bip30.ts";
import { checkBip34CoinbaseHeight } from "~/chain/bips/bip34.ts";
import { StoredPrevOutTxId } from "~/codec/stored/StoredPrevOutTxId.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { WireTxs } from "~/codec/wire/WireTxs.ts";
import { COINBASE_TXID, MAX_BLOCK_SIZE } from "~/constants.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { MessagePortLike } from "~/libs/message/mod.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";

console.log("[chain] booting");

// Stores this worker OWNS: it is the only writer, and it pins ONLY these. The
// header / blockhash stores belong to the p2p worker's domain — this worker
// never writes them (p2p persists headers straight to mmap; we only consume the
// block bodies it forwards). See Atomic.pin(names) for why the split matters.
const CHAIN_STORES = ["block", "tx", "txid", "pubkey", "spender"] as const;

const p2pMessageQueue = new Queue<{ type: string; data: any }>(1000);
const chunkQueue = new Queue<Uint8Array>(256);

// ── blocks/s throughput ──────────────────────────────────────────────────────
// Two views: `window` blocks since the last report (instantaneous rate) and a
// running total since boot (average rate). Reported at most once per interval.
const RATE_REPORT_INTERVAL_MS = 2_000;
const bootAt = performance.now();
let totalBlocks = 0;
let windowBlocks = 0;
let lastReportAt = bootAt;

/** Record `n` freshly-committed blocks and log blocks/s once per interval. */
function recordBlocks(n: number, tipHeight: number): void {
	totalBlocks += n;
	windowBlocks += n;
	const now = performance.now();
	const windowMs = now - lastReportAt;
	if (windowMs < RATE_REPORT_INTERVAL_MS) return;
	const windowRate = windowBlocks / (windowMs / 1000);
	const avgRate = totalBlocks / ((now - bootAt) / 1000);
	console.log(
		`[chain] ${windowRate.toFixed(0)} blocks/s (avg ${avgRate.toFixed(0)}/s) height=${tipHeight} total=${totalBlocks}`,
	);
	windowBlocks = 0;
	lastReportAt = now;
}

self.onmessage = async (event) => {
	console.log("[chain] main-port message event, ports:", event.ports.length, "data:", event.data);
	const port = event.ports[0]!;
	prepare(port);
	port.start();

	while (true) {
		try {
			await tick(port);
		} catch (error) {
			console.error("[chain] tick error:", error);
		}
	}
};

self.onunhandledrejection = (e) => {
	console.error("[chain] unhandledrejection:", e.reason);
};

self.postMessage(null);

function prepare(port: MessagePortLike): void {
	port.addEventListener("message", (event) => p2pMessageQueue.enqueue(event.data));

	// Tell p2p where our block data ends so it downloads from the next height.
	// (Headers are p2p's own domain now — it reads them from mmap, we don't send
	// or receive any header messages here.)
	const target = chainStore.stores.block.size() - 1;
	console.log(`[chain] sync port received, blocks committed up to height ${target}, requesting from p2p`);
	port.postMessage({ type: "seek", data: target });
	port.postMessage({ type: "start" });
}

async function tick(port: MessagePortLike): Promise<void> {
	const message = p2pMessageQueue.dequeue();
	if (!message) {
		if (chunkQueue.size() > 0) {
			await consumeChunks(port);
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
		await consumeChunks(port);
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
async function consumeChunks(port: MessagePortLike): Promise<void> {
	try {
		const chunk = chunkQueue.dequeue();
		if (!chunk) return;

		// tx is a raw BlobStore. writeInto() fills bytes AHEAD of the cursor
		// without moving it, so we track the offset ourselves and advance the
		// cursor once at the end (see the resize() below). Start at the next slot.
		let txStoreOffset = chainStore.stores.tx.nextItemPointer(MAX_BLOCK_SIZE);

		let blocksInChunk = 0;
		let offset = 0;
		while (offset < chunk.length) {
			const [block, size] = WireTxs.decode(chunk.subarray(offset));
			offset += size;
			blocksInChunk++;

			// Align to a block slot: nextItemPointer bumps us to the next chunk if
			// this one has less than a max block left, so the whole block region
			// (count + every tx) lands contiguously in one chunk — no straddle.
			txStoreOffset = chainStore.stores.tx.nextItemPointer(MAX_BLOCK_SIZE, txStoreOffset);

			// The height of THIS block is the next free block-store slot (block[i]
			// is the block at height i; genesis is pre-seeded at 0 by the header
			// domain but bodies start after it, so block.size() == the height we're
			// about to write). Captured before push() for the consensus checks.
			const height = chainStore.stores.block.size();

			// BIP34: from height 227931 the coinbase scriptSig must start with the
			// serialized block height. The coinbase is always the block's first tx.
			const coinbase = block[0];
			if (coinbase) checkBip34CoinbaseHeight(height, coinbase.inputs[0]?.scriptSig ?? new Uint8Array(0));

			// BIP30: is this one of the two historical blocks allowed to overwrite
			// an earlier identical coinbase txid? Only ever possible at two known
			// heights, so skip the (per-block) header hash recompute otherwise, and
			// verify against the stored header hash so we can't be tricked into
			// overwriting on the wrong chain.
			const bip30Overwrite = BIP30_EXCEPTION_BLOCKS.has(height) &&
				isBip30Exception(height, chainStore.stores.header.get(height)?.hash() ?? new Uint8Array(0));

			// block[height] -> pointer to this block's first txid entry.
			chainStore.stores.block.push({
				txPointer: txStoreOffset,
				wireSize: size + WireBlockHeader.stride.size,
				txCount: block.length,
				reward: 123, // TODO: calculate later.
			});

			for (const tx of block) {
				// BIP30 duplicate-txid handling. Normally `put` rejects duplicates —
				// which is correct, because BIP34 guarantees uniqueness from 227931
				// on and no earlier duplicate exists except the two known pairs. For
				// those two exception blocks, Core lets the new coinbase OVERWRITE the
				// earlier identical one (whose outputs were already spent): update the
				// existing txid entry's pointer in place and reuse its entry pointer
				// instead of inserting a second one.
				let txIdPointer: number;
				const existing = bip30Overwrite ? chainStore.stores.txid.getPointer(tx.txId) : undefined;
				if (existing !== undefined) {
					chainStore.stores.txid.setValue(existing, txStoreOffset);
					txIdPointer = existing;
				} else {
					txIdPointer = chainStore.stores.txid.put(tx.txId, txStoreOffset);
				}
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

		recordBlocks(blocksInChunk, chainStore.stores.block.size() - 1);

		// Ack so p2p's postedChunks - consumedChunks backpressure can drain.
		port.postMessage({ type: "consume" });
	} catch (reason) {
		console.error(`[chain] consumeChunks:`, reason);
		Deno.kill(Deno.pid);
	}
}
