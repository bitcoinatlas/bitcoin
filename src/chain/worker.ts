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
import { verifyProofOfWork, workFromHeader } from "~/libs/bitcoin/pow.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { HashMapStore } from "~/libs/storage/HashMapStore.ts";
import { MessagePortLike } from "~/libs/message/mod.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";

console.log("[chain] booting");

// This worker is the SINGLE pinner: it owns every store's durable commit. p2p
// is pure networking now — it reads headers from shared mmap to drive downloads
// and builds locators, but never writes or pins. Header application (reorg-aware
// most-work adoption) and block-body indexing both land here, so one worker holds
// the manifest write lock and there is no cross-isolate BEGIN IMMEDIATE contention.

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

// ── header chain (this worker owns the writes now) ────────────────────────────
// p2p forwards raw headers it fetched from peers; this worker validates, applies
// the most-work rule, writes header + blockhash mmap, and pins. p2p reads the
// same mmap to build locators and drive block download. Reads up to size() here
// are always this worker's own committed writes.

function tipHeight(): number {
	return chainStore.stores.header.size() - 1;
}

function headerAt(height: number): WireBlockHeader | undefined {
	return chainStore.stores.header.get(height);
}

function headerHashAt(height: number): Uint8Array | undefined {
	return chainStore.stores.header.get(height)?.hash();
}

function tipHash(): Uint8Array | undefined {
	const height = tipHeight();
	return height < 0 ? undefined : headerHashAt(height);
}

/**
 * hash -> height, VERIFIED against the header store. A reorg leaves stale
 * blockhash rows for orphaned headers (the hashmap has no delete); this check —
 * "does the header now sitting at that height still have this hash?" — makes
 * every such stale row read back as unknown. Self-healing, no cleanup pass.
 */
function heightOfHash(hash: Uint8Array): number | undefined {
	const height = chainStore.stores.blockhash.get(hash);
	if (height === undefined) return undefined;
	const at = headerHashAt(height);
	return at && equals(at, hash) ? height : undefined;
}

/** Outcome of applying a batch: how many headers adopted, and — on a reorg that
 * drops below already-downloaded bodies — the split height p2p must rewind its
 * download cursor to. rewind is only set when p2p needs to act on it. */
type ApplyResult = { adopted: number; rewind?: number };

/**
 * Adopt a peer's header branch iff it out-works ours (most-work rule):
 *   1. find where their branch attaches to our chain (split point).
 *   2. validate the incoming branch (links + PoW) and sum its work.
 *   3. sum OUR work above the same split. Shared prefix cancels — branch work
 *      alone decides it.
 *   4. if the peer's branch strictly out-works ours, drop to the split and
 *      append it; otherwise keep ours.
 *
 * Pins on adoption. Returns how many headers we adopted (0 = kept ours) plus a
 * rewind height when a reorg invalidated already-downloaded block bodies.
 */
function applyHeaders(headers: WireBlockHeader[]): ApplyResult {
	const head = headers[0];
	if (!head) return { adopted: 0 };

	// The header chain is never empty in normal operation — main seeds + pins
	// genesis before spawning workers, and recover() reveals it at import. Bail
	// rather than dereferencing a -1 tip if we're somehow called early.
	const tip = tipHash();
	if (tip === undefined) return { adopted: 0 };

	// 1. split point
	let splitHeight: number;
	if (equals(head.prevHash, tip)) {
		splitHeight = tipHeight(); // plain extension
	} else {
		const forked = heightOfHash(head.prevHash);
		if (forked === undefined) return { adopted: 0 }; // attaches to nothing we know
		splitHeight = forked;
	}

	// 2. validate the incoming branch, sum its work
	const branch: WireBlockHeader[] = [];
	let prevHash = headerHashAt(splitHeight)!; // == head.prevHash
	let branchWork = 0n;
	for (const header of headers) {
		if (!equals(header.prevHash, prevHash)) break; // stop at first non-link
		if (!verifyProofOfWork(header)) break;
		branch.push(header);
		branchWork += workFromHeader(header);
		prevHash = header.hash();
	}
	if (branch.length === 0) return { adopted: 0 };

	// 3. our work above the same split (empty for a plain extension)
	const currentTip = tipHeight();
	let ourWork = 0n;
	for (let h = splitHeight + 1; h <= currentTip; h++) ourWork += workFromHeader(headerAt(h)!);

	// 4. most-work rule — only switch on a strict win
	const isReorg = splitHeight < currentTip;
	if (isReorg && branchWork <= ourWork) return { adopted: 0 };

	let rewind: number | undefined;
	if (isReorg) {
		console.log(`[chain] header reorg: dropping height ${tipHeight()} -> ${splitHeight}, applying ${branch.length} headers`);
		chainStore.stores.header.reveal(splitHeight + 1); // stale blockhash rows self-heal via heightOfHash

		// A reorg below where block bodies were already committed means those
		// bodies are now orphaned and the block/tx/txid/pubkey/spender domain must
		// be rewound too. That reverse-replay is NOT implemented yet — throw rather
		// than silently serve an inconsistent chain. For IBD off a trusted peer
		// this never fires. Header-only reorgs above the block tip are fine.
		if (splitHeight < chainStore.stores.block.size() - 1) {
			throw new Error(
				`header reorg to ${splitHeight} is below committed block tip ${chainStore.stores.block.size() - 1}; ` +
					`block-domain rewind is not implemented`,
			);
		}

		// Tell p2p to pull its download cursor back to the split and refetch.
		rewind = splitHeight;
	}

	for (const header of branch) {
		const height = chainStore.stores.header.stage(header);
		chainStore.stores.header.reveal(height + 1);

		const offset = chainStore.stores.blockhash.next(chainStore.stores.blockhash.size());
		const size = chainStore.stores.blockhash.stage(header.hash(), height, offset);
		chainStore.stores.blockhash.reveal(offset + size);
	}
	chainStore.manifest.pin();
	return { adopted: branch.length, rewind };
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

	// Tell p2p where our block bodies end so it downloads from the next height.
	// Headers now flow the other way too: p2p fetches them and forwards raw
	// batches here (type "headers"); this worker applies + pins them and acks.
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

	if (message.type === "headers") {
		// p2p fetched these from a peer and forwarded the raw batch. We own the
		// header writes now: validate + apply the most-work rule + pin, then ack
		// so p2p knows the new tip is durably readable before it builds its next
		// locator. The ack carries the adoption count (0 = p2p stops asking this
		// peer) and, on a reorg, the height p2p must rewind its download to.
		const [headers] = WireBlockHeaders.decode(message.data as Uint8Array);
		let result: ApplyResult;
		try {
			result = applyHeaders(headers);
		} catch (reason) {
			console.error("[chain] applyHeaders failed:", reason);
			Deno.kill(Deno.pid);
			return;
		}
		port.postMessage({ type: "headers-applied", data: result });
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
		const spender = chainStore.stores.spender;
		// tx is a raw BlobStore. stage() fills bytes AHEAD of the cursor without
		// moving it, so we track the offset ourselves and advance the cursor once
		// at the end (see the reveal() below). Start at the next slot.
		let txPointer = txStore.next(MAX_BLOCK_SIZE);

		// Running global output count. Seeded from spender's COMMITTED size (the
		// number of output slots pinned by prior chunks) and advanced per tx as we
		// go — the in-memory base each tx's outputs start at, no store read. Each
		// tx records this base as firstOutputHeight in its txid entry so a later
		// spend recovers any output's global height as firstOutputHeight + vout.
		// (Under parallel consume this is exactly the per-range base a worker keeps
		// in memory, seeded from the previous range's completion checkpoint.)
		let total = spender.size();

		let blocksInChunk = 0;
		let offset = 0;
		while (offset < chunk.length) {
			const [block, size] = WireTxs.decode(chunk.subarray(offset));
			offset += size;
			blocksInChunk++;

			// Align to a block slot: next() bumps us to the next chunk if this one
			// has less than a max block left, so the whole block region (count +
			// every tx) lands contiguously in one chunk — no straddle.
			txPointer = txStore.next(MAX_BLOCK_SIZE, txPointer);
			// This reservation covers the WHOLE block, once — not a fresh
			// MAX_BLOCK_SIZE for every tx inside it. blockRegionEnd is the actual
			// boundary that reservation bought us; each tx below asks for what's
			// left of it, not the full amount again.
			const blockRegionEnd = txPointer + MAX_BLOCK_SIZE;

			// The height of THIS block is the next free block-store slot (block[i]
			// is the block at height i; genesis is pre-seeded at 0 by the header
			// domain but bodies start after it, so block.size() == the height we're
			// about to write). Captured before stage() for the consensus checks.
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

			// block[height] -> pointer to this block's first tx entry.
			chainStore.stores.block.stage({
				txPointer,
				wireSize: size + WireBlockHeader.stride.size,
				txCount: block.length,
				reward: 123_456_789, // TODO: calculate later.
			}, height);
			chainStore.stores.block.reveal(height + 1);

			for (const tx of block) {
				// This tx's outputs occupy [total, total + outputs.length) in global
				// output-height space. Recorded as firstOutputHeight in the txid entry
				// so a spend of any of them resolves height via one txid lookup.
				const totalOutput = total;

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
				const txIdPointer = putEntry(chainStore.stores.txid, tx.txId, { totalOutput, txPointer });

				const storedTx: Codec.InferInput<typeof StoredTx> = {
					lockTimeAndVersionPack: { locktime: tx.locktime, version: tx.version },
					inputs: tx.inputs.map((input, index): Codec.InferInput<typeof StoredTxInput> => {
						let prevOutTxId: Codec.InferInput<typeof StoredPrevOutTxId>;
						if (equals(input.prevOut.txId, COINBASE_TXID)) {
							prevOutTxId = null;
						} else {
							// One txid lookup gives both the prevout's entry pointer (what we
							// store as prevOutTxId) and its value — which now carries the
							// prevout tx's firstOutputHeight, so the spent output's global
							// height is firstOutputHeight + vout with no blob read.
							const resolved = chainStore.stores.txid.getValueAndPointer(input.prevOut.txId);
							if (resolved === undefined) {
								throw new Error("prevOut references a txid not present in the index");
							}
							const [prevValue, prevEntryPointer] = resolved;
							const prevOutputIndex = prevValue.totalOutput + input.prevOut.output;
							spender.item.encodeInto(txIdPointer, spender.mmap(prevOutputIndex));
							prevOutTxId = prevEntryPointer;
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

				// This tx's outputs are now accounted for — advance the global base.
				total += tx.outputs.length;

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
				txPointer += StoredTx.encodeInto(storedTx, txStore.mmap(blockRegionEnd - txPointer, txPointer));
			}
		}

		// Commit the tx blob: advance its cursor past everything we wrote so the
		// bytes become live/readable (the txid pointers we stored point into this
		// range). Extend the spender array to cover every output created this chunk
		// (new slots read 0 = unspent). Then pin OUR domain only — one consistent
		// snapshot per round.
		txStore.reveal(txPointer);
		spender.reveal(total);
		chainStore.manifest.pin();

		recordBlocks(blocksInChunk, chainStore.stores.block.size() - 1);

		// Ack so p2p's postedChunks - consumedChunks backpressure can drain.
		port.postMessage({ type: "consume" });
	} catch (reason) {
		console.error(`[chain] consumeChunks:`, reason);
		Deno.kill(Deno.pid);
	}
}
