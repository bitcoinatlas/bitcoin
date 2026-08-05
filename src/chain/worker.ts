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
import { HashMapStore } from "~/libs/storage/HashMapStore.ts";
import { MessagePortLike } from "~/libs/message/mod.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";

console.log("[chain] booting");

// Stores this worker OWNS: it is the only writer, and it pins ONLY these. The
// header / blockhash stores belong to the p2p worker's domain — this worker
// never writes them (p2p persists headers straight to mmap; we only consume the
// block bodies it forwards). See Manifest.pin(names) for why the split matters.
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

	chainStore.stores.tx.startArchiveWorkers({
		maxRestoredChunks: 8,
		zstd: {
			archive: {
				compressionLevel: 19,
				enableLongDistanceMatching: 1,
				windowLog: 27, // maybe make it 24 later?
				checksumFlag: 1, // 4-byte frame checksum, cheap integrity guard
				contentSizeFlag: 1, // size in frame header — works on the sync path,
			},
			restore: {
				windowLogMax: 27,
			},
		},
	});

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
 * Append a (key, value) to a HashMapStore and return its entry pointer.
 *
 * The store's commit model is stage → reveal → pin. `stage` writes the entry
 * bytes at the next free slot but doesn't make it findable; a manual `reveal`
 * stages it in the per-worker index so our OWN later `get`/`getPointer` in this
 * same chunk can see it (prevOut lookups, BIP30 overwrite checks) before the
 * round-ending `pin()` wires it into the shared buckets. The entry offset — the
 * value `getPointer` returns — is the stable pointer we store elsewhere.
 */
function putEntry<K extends Codec, V extends Codec>(
	store: HashMapStore<K, V>,
	key: Codec.InferInput<K>,
	value: Codec.InferInput<V>,
): number {
	const offset = store.next(store.size());
	const size = store.stage(key, value, offset);
	store.reveal(offset + size);
	return offset;
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

		const txStore = chainStore.stores.tx;
		// tx is a raw BlobStore. stage() fills bytes AHEAD of the cursor without
		// moving it, so we track the offset ourselves and advance the cursor once
		// at the end (see the reveal() below). Start at the next slot.
		let txStoreOffset = txStore.next(MAX_BLOCK_SIZE);

		// block[i] is the block at height i; genesis is pre-seeded at 0 by the
		// header domain but bodies start after it, so block.size() == the height
		// of the first body we're about to write. The block cursor is now revealed
		// ONCE at the end of the chunk (after the tx bytes it points into are
		// live), so it does not advance during this loop — track height locally.
		let height = chainStore.stores.block.size();

		let blocksInChunk = 0;
		let offset = 0;
		while (offset < chunk.length) {
			const [block, size] = WireTxs.decode(chunk.subarray(offset));
			offset += size;
			blocksInChunk++;

			// Align to a block slot: next() bumps us to the next chunk if this one
			// has less than a max block left, so the whole block region (count +
			// every tx) lands contiguously in one chunk — no straddle.
			txStoreOffset = txStore.next(MAX_BLOCK_SIZE, txStoreOffset);
			// This reservation covers the WHOLE block, once — not a fresh
			// MAX_BLOCK_SIZE for every tx inside it. blockRegionEnd is the actual
			// boundary that reservation bought us; each tx below asks for what's
			// left of it, not the full amount again.
			const blockRegionEnd = txStoreOffset + MAX_BLOCK_SIZE;

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

			// block[height] -> pointer to this block's first tx entry. Staged now,
			// but NOT revealed until the tx bytes it points into are live (end of
			// chunk) — a visible block entry pointing at an unrevealed tx region is
			// exactly what corrupted coinbase reads (empty inputs) downstream.
			chainStore.stores.block.stage({
				txPointer: txStoreOffset,
				wireSize: size + WireBlockHeader.stride.size,
				txCount: block.length,
				reward: 123, // TODO: calculate later.
			}, height);

			for (const tx of block) {
				// BIP30 duplicate-txid handling. The store has no in-place update or
				// delete, but commit() prepends fresh entries to their bucket head
				// (and our per-worker stage overwrites the staged offset), so simply
				// appending a new entry for a duplicate txid makes it win every
				// read — exactly the OVERWRITE Core does for the two exception
				// blocks. For every other block BIP34 guarantees uniqueness, so a
				// duplicate never legitimately happens here.
				if (bip30Overwrite && chainStore.stores.txid.getPointer(tx.txId) !== undefined) {
					console.log(`[chain] BIP30 overwrite of duplicate coinbase txid at height ${height}`);
				}
				const txIdPointer = putEntry(chainStore.stores.txid, tx.txId, txStoreOffset);

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
							putEntry(chainStore.stores.spender, { tx: pointer, output: input.prevOut.output }, txIdPointer);
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
						// The pubkey store dedups scripts: each distinct scriptPubKey is
						// stored ONCE and its entry offset is the stable pointer we hand
						// out. First sighting -> stage it (value = this tx). Reuse ->
						// keep the existing pointer and record its stored last-tx
						// pointer as this output's previousOutputTx link.
						const pubkeyResult = chainStore.stores.pubkey.getValueAndPointer(output.scriptPubKey);
						if (!pubkeyResult) {
							const pubKeyPointer = putEntry(chainStore.stores.pubkey, output.scriptPubKey, txIdPointer);
							return {
								value: Number(output.value),
								previousOutputTx: null,
								scriptPubKey: pubKeyPointer,
							};
						}
						// getValueAndPointer returns [value, pointer]: value is the
						// pubkey's last-tx pointer, pointer is the pubkey entry itself
						// (== the scriptPubKey pointer we store). The store has no
						// in-place update, so the linked list can't be advanced here;
						// we record the previous last-tx pointer and reuse the entry.
						const [previousTxIdPointer, pubKeyPointer] = pubkeyResult;
						return {
							value: Number(output.value),
							previousOutputTx: previousTxIdPointer,
							scriptPubKey: pubKeyPointer,
						};
					}),
				};

				// The block-level next(MAX_BLOCK_SIZE, ...) above reserves room for the
				// whole block up front, once — blockRegionEnd is that reservation's real
				// boundary. Each tx write asks for what's LEFT of the block's budget
				// (blockRegionEnd - txStoreOffset), not a fresh MAX_BLOCK_SIZE every
				// time — asking for the full amount on every tx demanded room the
				// reservation never promised past the first tx, and threw spuriously
				// partway through any block that used more than a sliver of its budget.
				// If the block's actual total size ever exceeds what was reserved, this
				// still throws — correctly, right here — instead of silently truncating
				// the tx into whatever chunk space happened to remain, which is what
				// was producing the corrupted, permanently-broken coinbase reads
				// downstream.
				txStoreOffset += StoredTx.encodeInto(storedTx, txStore.mmap(blockRegionEnd - txStoreOffset, txStoreOffset));
			}

			height++;
		}

		// Commit ordering is load-bearing: reveal the tx blob FIRST so the bytes
		// every txPointer references are live, THEN reveal the block cursor over
		// the entries that point into them. Revealing block before tx (as the old
		// per-block reveal did) briefly exposed block entries whose coinbase tx
		// bytes weren't committed yet — reads decoded zeros into an empty inputs
		// array and crashed on inputs[0]. Both cursors settle before pin() takes
		// the round's snapshot and broadcasts it to reader isolates.
		txStore.reveal(txStoreOffset);
		chainStore.stores.block.reveal(height);
		chainStore.manifest.pin(CHAIN_STORES);

		recordBlocks(blocksInChunk, chainStore.stores.block.size() - 1);

		// Ack so p2p's postedChunks - consumedChunks backpressure can drain.
		port.postMessage({ type: "consume" });
	} catch (reason) {
		console.error(`[chain] consumeChunks:`, reason);
		Deno.kill(Deno.pid);
	}
}
