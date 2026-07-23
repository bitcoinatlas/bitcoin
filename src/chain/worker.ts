import { delay } from "@std/async";
import { concat } from "@std/bytes";
import { encodeHex } from "@std/encoding";
import { chainStorage } from "~/chain/ChainStorage.ts";
import { EncodedBlock } from "~/chain/consume.worker.ts";
import { SpenderIndexer } from "~/chain/SpenderIndexer.ts";
import { StoredPrevOutTxId } from "~/codec/stored/StoredPrevOutTxId.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { SECOND } from "~/constants.ts";
import { PARALLELISM_THREADS } from "~/env.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { Uint8ArrayMap } from "~/libs/collections/Uint8ArrayMap.ts";
import { MessagePortLike } from "~/libs/message/mod.ts";

/** consume.worker `init` output: unknown scriptPubKeys, pre-hashed + pre-encoded. */
type InitResult = {
	/** hash of each unknown pubkey, packed 32 bytes each (for cross-worker dedup). */
	hashes: Uint8Array;
	/** StoredScriptPubKey bytes of each unknown pubkey, back-to-back. */
	encoded: Uint8Array;
	/** encoded length of each unknown pubkey; slices `encoded`. */
	lengths: Uint32Array;
};

/** Human-readable ETA. `—` when not yet computable (no rate, or already at tip). */
function formatEta(seconds: number): string {
	if (!isFinite(seconds) || seconds <= 0) return "—";
	const h = Math.floor(seconds / 3600);
	const m = Math.floor((seconds % 3600) / 60);
	if (h >= 24) return `${Math.floor(h / 24)}d${h % 24}h`;
	return h > 0 ? `${h}h${m}m` : `${m}m`;
}

/**
 * Per-round throughput bookkeeping. Reports three numbers:
 *   overall — totalTxs / totalElapsed. Lifetime mean; drifts vs reality as block
 *             density climbs (carries genesis-era ballast). Context only.
 *   current — Δtx/Δt over the last N rounds. Tracks the density ramp in near real
 *             time; a single-round Δ is too jittery, a short window isn't.
 *   ETA     — remainingBlocks / current blocks-per-sec. Block-based because the tx
 *             counts of unreached blocks are unknown in-process. OPTIMISTIC across
 *             the ramp (blocks/s falls into denser years) — treat as a lower bound.
 */
class ThroughputMeter {
	private static readonly WINDOW_ROUNDS = 12;
	private readonly startTime = performance.now();
	private lastRoundTime = performance.now();
	private totalTxs = 0;
	private readonly window: Array<{ txs: number; blocks: number; ms: number }> = [];

	/** Record a finished round; returns the formatted throughput/ETA tail. */
	record(committedBlocks: number, committedTxs: number, currentHeight: number, targetHeight: number): string {
		this.totalTxs += committedTxs;
		const now = performance.now();
		const roundMs = now - this.lastRoundTime;
		this.lastRoundTime = now;

		this.window.push({ txs: committedTxs, blocks: committedBlocks, ms: roundMs });
		if (this.window.length > ThroughputMeter.WINDOW_ROUNDS) this.window.shift();
		let winTxs = 0, winBlocks = 0, winMs = 0;
		for (const s of this.window) {
			winTxs += s.txs;
			winBlocks += s.blocks;
			winMs += s.ms;
		}

		const overallSec = (now - this.startTime) / SECOND;
		const overallRate = overallSec > 0 ? (this.totalTxs / overallSec) | 0 : 0;
		const currentRate = winMs > 0 ? ((winTxs / winMs) * SECOND) | 0 : 0;
		const blocksPerSec = winMs > 0 ? (winBlocks / winMs) * SECOND : 0;

		const remainingBlocks = Math.max(0, targetHeight - currentHeight);
		const etaSec = blocksPerSec > 0 ? remainingBlocks / blocksPerSec : 0;

		return `overall ${overallRate} tx/s · current ${currentRate} tx/s · ` +
			`${blocksPerSec.toFixed(1)} blk/s | remaining=${remainingBlocks} ETA ${formatEta(etaSec)}`;
	}
}

// TODO: This whole thing is messy, kinda hooked it up with new stuff.
// TODO: This is basically part of the old version of now ChainStorage.
class ChainStore {
	public readonly blockHashToHeightMap: Uint8ArrayMap<number>;

	private p2pChannel: MessagePortLike;
	private p2pMessageQueue: Queue<{ type: string; data: any }>;

	// Indexes who-spends-what off the commit path, on committed data from earlier
	// rounds. Kicked (fire-and-forget) each round so it soaks up the sync-point
	// gap; catches up to the tip once p2p goes idle.
	private spenderIndexer: SpenderIndexer;

	private consumers: Worker[];
	// Block chunks buffered from p2p, drained N-at-a-time into commit batches.
	// Decoupled from p2p's prefetch depth: p2p can run ahead filling this while a
	// batch commits. A chunk arriving is just an enqueue — it touches no barrier
	// state, so a later batch can't corrupt an in-flight one (the old bug).
	private chunkQueue: Queue<Uint8Array>;
	private batchSize: number;
	// Resolves once every consumer has posted "ready" (module loaded, rocksdb
	// open, message handler attached). Gate the first batch on this — a message
	// posted to a still-loading worker can be dropped, hanging its init promise.
	private consumersReady: PromiseWithResolvers<void>;
	private readyCount = 0;
	private round = 0;
	private readonly meter = new ThroughputMeter();

	private constructor(p2pChannel: MessagePortLike, initialHeaders: WireBlockHeader[]) {
		this.p2pChannel = p2pChannel;
		this.p2pMessageQueue = new Queue(1000);
		this.blockHashToHeightMap = new Uint8ArrayMap<number>(Math.max(256, initialHeaders.length * 2));
		for (let index = 0; index < initialHeaders.length; index++) {
			const header = initialHeaders[index]!;
			const hash = header.hash();
			this.blockHashToHeightMap.set(hash, index);
			chainStorage.indexHeightByHash(hash, index);
		}

		this.consumers = new Array(PARALLELISM_THREADS);
		this.batchSize = this.consumers.length;
		this.chunkQueue = new Queue(256);
		this.consumersReady = Promise.withResolvers<void>();
		for (let i = 0; i < this.consumers.length; i++) {
			const worker = new Worker(new URL("./consume.worker.ts", import.meta.url), { type: "module", name: `consumer-${i}` });
			// Surface worker failures loudly. A throw inside init/process must not
			// silently strand a batch's Promise.all; fail fast instead.
			worker.addEventListener("error", (event) => {
				console.error(`[chain] consumer-${i} uncaught:`, event.message, event.filename, event.lineno);
				Deno.exit(1);
			});
			worker.addEventListener("message", (event) => {
				const stage = (event.data as { stage?: string })?.stage;
				if (stage === "ready") {
					if (++this.readyCount === this.consumers.length) this.consumersReady.resolve();
					return;
				}
				if (stage !== "error") return;
				const e = event.data as { phase: string; message: string; stack?: string };
				console.error(`[chain] consumer-${i} threw in ${e.phase}: ${e.message}\n${e.stack ?? ""}`);
				Deno.exit(1);
			});
			this.consumers[i] = worker;
		}

		this.spenderIndexer = new SpenderIndexer();
	}

	static start(p2pChannel: MessagePortLike): ChainStore {
		const headers = chainStorage.stores.headers.slice(0, chainStorage.stores.headers.length());
		const store = new ChainStore(p2pChannel, headers);

		p2pChannel.addEventListener("message", (event) => store.p2pMessageQueue.enqueue(event.data));
		const startData = WireBlockHeaders.encode(headers);
		p2pChannel.postMessage({ type: "seek", data: chainStorage.stores.blocks.length() - 1 });
		p2pChannel.postMessage({ type: "start", data: startData }, [startData.buffer]);
		return store;
	}

	async tick(): Promise<void> {
		const message = this.p2pMessageQueue.dequeue();
		if (!message) {
			// Nothing pending. If chunks are waiting but we never reached a full
			// batch (tail of IBD, or p2p idle at the tip), flush the partial now so
			// it doesn't stall. During fast IBD the queue reaches batchSize before
			// it ever empties, so this only fires when p2p genuinely has nothing.
			if (this.chunkQueue.size() > 0) {
				await this.runBatch();
			} else {
				// p2p has nothing — at or near the tip. Let the spender index finish
				// catching up to the last committed block.
				void this.spenderIndexer.catchUp(chainStorage.stores.blocks.length());
				await delay(0);
			}
			return;
		}

		if (message.type === "blocks") {
			if (!this.chunkQueue.enqueue(message.data as Uint8Array)) {
				console.error("[chain] chunkQueue overflow — p2p backpressure is not holding");
				Deno.exit(1);
			}
			if (this.chunkQueue.size() >= this.batchSize) await this.runBatch();
			return;
		}

		if (message.type === "headers") {
			const [headers] = WireBlockHeaders.decode(message.data);
			await this.handleHeadersMessage(headers);
			return;
		}

		if (message.type === "reorg") {
			this.handleReorgMessage(message.data as number);
			return;
		}
	}

	private handleReorgMessage(_keepHeight: number): void {
		throw new Error("Not Implemented");
	}

	private async handleHeadersMessage(headers: WireBlockHeaders) {
		try {
			let height = chainStorage.stores.headers.length();
			await chainStorage.atomic.trx((stores) => {
				for (const header of headers) {
					height = stores.headers.push(header);
					const hash = header.hash();
					this.blockHashToHeightMap.set(hash, height);
					chainStorage.indexHeightByHash(hash, height);
				}
			});
			return { height };
		} catch (reason) {
			console.error("Failed to append block header:", reason);
			Deno.exit(1);
		}
	}

	/** Drain up to batchSize chunks for the next commit batch. */
	private drainBatch(): Uint8Array[] {
		const n = Math.min(this.batchSize, this.chunkQueue.size());
		const batch: Uint8Array[] = new Array(n);
		for (let i = 0; i < n; i++) batch[i] = this.chunkQueue.dequeue()!;
		return batch;
	}

	/** Await one stage round-trip from a consumer; rejects on its `error` stage. */
	private awaitStage(worker: Worker, id: number, sendStage: string, doneStage: string, payload: ArrayBufferView): Promise<any> {
		return new Promise((resolve, reject) => {
			const onMessage = (event: MessageEvent) => {
				const data = event.data as { stage: string; message?: string };
				if (data.stage === "error") {
					worker.removeEventListener("message", onMessage);
					reject(new Error(`consumer-${id} ${sendStage}: ${data.message}`));
					return;
				}
				if (data.stage !== doneStage) return;
				worker.removeEventListener("message", onMessage);
				resolve(event.data);
			};
			worker.addEventListener("message", onMessage);
			worker.postMessage({ stage: sendStage, data: payload }, [payload.buffer as ArrayBuffer]);
		});
	}

	/** Stage 1: worker decodes its chunk, returns unknown scriptPubKeys. */
	private async initWorker(worker: Worker, chunk: Uint8Array, id: number): Promise<InitResult> {
		const { hashes, encoded, lengths } = await this.awaitStage(worker, id, "init", "init-done", chunk);
		return { hashes, encoded, lengths };
	}

	/** Stage 2: worker encodes blocks with assigned pubkey pointers. */
	private async processWorker(worker: Worker, pointers: BigUint64Array, id: number): Promise<EncodedBlock[]> {
		const { blocks } = await this.awaitStage(worker, id, "process", "process-done", pointers);
		return blocks;
	}

	/**
	 * Assign each unknown pubkey a prospective blob offset (replayed and asserted
	 * at commit) and build ONE deduped blob of the genuinely-new ones for a single
	 * append. Workers already hashed + encoded them, so this only dedups and copies
	 * subarrays — no sha256, no encode on the chain thread.
	 */
	private assignPubkeys(pubkeysPerWorker: InitResult[]): {
		pointersPerWorker: BigUint64Array[];
		pubkeyBlob: Uint8Array;
		pubkeyBase: number;
		newHashes: Uint8Array[];
		newPointers: number[];
	} {
		const cache = new FastUint8ArrayMap<number>();
		const newHashes: Uint8Array[] = [];
		const newPointers: number[] = [];
		const blobParts: Uint8Array[] = [];
		const pubkeyBase = chainStorage.stores.pubkeys.size();
		let pubkeyCursor = pubkeyBase;
		const pointersPerWorker: BigUint64Array[] = new Array(pubkeysPerWorker.length);

		for (let i = 0; i < pubkeysPerWorker.length; i++) {
			const { hashes, encoded, lengths } = pubkeysPerWorker[i]!;
			const n = lengths.length;
			const pointers = new BigUint64Array(n);
			let encOffset = 0;
			for (let j = 0; j < n; j++) {
				const len = lengths[j]!;
				const hash = hashes.subarray(j * 32, j * 32 + 32);
				let ptr = cache.get(hash);
				if (ptr === undefined) {
					ptr = chainStorage.stores.pubkey.get(hash);
					if (ptr === undefined) {
						ptr = pubkeyCursor;
						pubkeyCursor += len;
						blobParts.push(encoded.subarray(encOffset, encOffset + len));
						newHashes.push(hash);
						newPointers.push(ptr);
					}
					cache.put(hash, ptr);
				}
				pointers[j] = BigInt(ptr);
				encOffset += len;
			}
			pointersPerWorker[i] = pointers;
		}

		return { pointersPerWorker, pubkeyBlob: concat(blobParts), pubkeyBase, newHashes, newPointers };
	}

	/**
	 * Assign block/tx pointers, register txIds so in-batch prevOuts resolve, patch
	 * them, and concatenate every block into ONE blob. All prospective offsets are
	 * relative to the current store sizes and asserted at commit. No disk writes.
	 */
	private prepareBlocks(blocksPerWorker: EncodedBlock[][]): {
		blockBlob: Uint8Array;
		blockBases: number[];
		txBase: number;
		txidEntries: [txid: Uint8Array, pointer: number][];
		committedBlocks: number;
		committedTxs: number;
	} {
		const txBase = chainStorage.stores.txs.size();
		const batchTxid = new FastUint8ArrayMap<number>();
		const txidEntries: [Uint8Array, number][] = [];
		const blockBases: number[] = [];
		let committedBlocks = 0;
		let committedTxs = 0;
		let cursor = txBase;

		// Pass 1: pointer = block base + tx offset within its block. Register the
		// whole batch's txids before patching so cross-tx prevOuts within the batch
		// resolve against batchTxid rather than falling through to disk.
		for (let i = 0; i < blocksPerWorker.length; i++) {
			for (const block of blocksPerWorker[i]!) {
				blockBases.push(cursor);
				const txCount = block.txOffsets.length;
				for (let k = 0; k < txCount; k++) {
					const txId = block.txIds.subarray(k * 32, k * 32 + 32);
					const pointer = cursor + block.txOffsets[k]!;
					batchTxid.set(txId, pointer);
					txidEntries.push([txId, pointer]);
				}
				cursor += block.buffer.length;
				committedBlocks++;
				committedTxs += txCount;
			}
		}

		// Pass 2: patch prevOut pointers in place (in-batch first, then committed).
		for (let i = 0; i < blocksPerWorker.length; i++) {
			for (const block of blocksPerWorker[i]!) {
				const patchCount = block.patchOffsets.length;
				for (let p = 0; p < patchCount; p++) {
					const prevTxId = block.patchTxids.subarray(p * 32, p * 32 + 32);
					const pointer = batchTxid.get(prevTxId) ?? chainStorage.stores.txid.get(prevTxId);
					if (pointer === undefined) throw new Error(`unresolved prevOut at commit txid=${encodeHex(prevTxId)}`);
					StoredPrevOutTxId.patchPointer(block.buffer, block.patchOffsets[p]!, pointer);
				}
			}
		}

		const blockParts: Uint8Array[] = [];
		for (let i = 0; i < blocksPerWorker.length; i++) {
			for (const block of blocksPerWorker[i]!) blockParts.push(block.buffer);
		}

		return { blockBlob: concat(blockParts), blockBases, txBase, txidEntries, committedBlocks, committedTxs };
	}

	/**
	 * Process one batch of chunks: Parallel.For (init | process) with a serial join
	 * at commit. init and process run OUTSIDE the write transaction — pubkey
	 * pointers are prospective blob offsets asserted at commit — so the trx is short
	 * and synchronous and worker reads never race an open write. State is local.
	 */
	private async runBatch(): Promise<void> {
		// Never post work to workers that haven't loaded yet (dropped-message hang).
		await this.consumersReady.promise;
		const batch = this.drainBatch();
		if (batch.length === 0) return;

		const round = ++this.round;

		// Kick spender indexing for everything committed by earlier rounds. Runs in
		// its own workers on already-pinned data, overlapping the work below — the
		// sync-point gap the design targets. Fire-and-forget and non-reentrant.
		void this.spenderIndexer.catchUp(chainStorage.stores.blocks.length());

		// 1) init all chunks in parallel; each returns its unknown scriptPubKeys.
		const pubkeysPerWorker = await Promise.all(
			batch.map((chunk, i) => this.initWorker(this.consumers[i]!, chunk, i)),
		);

		// 2) assign pubkey pointers without touching disk (deduped across workers).
		const { pointersPerWorker, pubkeyBlob, pubkeyBase, newHashes, newPointers } = this.assignPubkeys(pubkeysPerWorker);

		// 3) process all chunks in parallel; each returns encoded blocks + patch meta.
		const blocksPerWorker = await Promise.all(
			batch.map((_, i) => this.processWorker(this.consumers[i]!, pointersPerWorker[i]!, i)),
		);

		// 4) assign block/tx pointers, patch in-batch prevOuts, concat into one blob.
		const { blockBlob, blockBases, txBase, txidEntries, committedBlocks, committedTxs } = this.prepareBlocks(blocksPerWorker);

		// 5) commit: pure batched writes.
		await chainStorage.atomic.trx((stores, trx) => {
			if (pubkeyBlob.length > 0) {
				const off = stores.pubkeys.append(pubkeyBlob);
				if (off !== pubkeyBase) throw new Error(`pubkey blob offset mismatch: expected ${pubkeyBase} got ${off}`);
			}
			const pubkeyEntries: [Uint8Array, number][] = new Array(newHashes.length);
			for (let u = 0; u < newHashes.length; u++) pubkeyEntries[u] = [newHashes[u]!, newPointers[u]!];
			stores.pubkey.setMany(pubkeyEntries, trx);

			stores.txid.setMany(txidEntries, trx);

			if (blockBlob.length > 0) {
				const off = stores.txs.append(blockBlob);
				if (off !== txBase) throw new Error(`txs blob offset mismatch: expected ${txBase} got ${off}`);
			}
			stores.blocks.pushMany(blockBases);
		});

		// 6) log throughput (see ThroughputMeter for what overall/current/ETA mean).
		const targetHeight = chainStorage.stores.headers.length() - 1;
		const currentHeight = chainStorage.stores.blocks.length() - 1;
		console.log(
			`[chain] round ${round} | blocks=${committedBlocks} txs=${committedTxs} ` +
				`height=${currentHeight}/${targetHeight} | ` +
				this.meter.record(committedBlocks, committedTxs, currentHeight, targetHeight),
		);

		// 7) let p2p refill: one consume per chunk we drained.
		for (let i = 0; i < batch.length; i++) this.p2pChannel.postMessage({ type: "consume" });
	}
}

// ── worker entry ──────────────────────────────────────────────────────────
// Top-level side effects: this file is the `chain` worker's entry point. The
// main thread spawns it and hands over a MessagePort in the first message.
self.addEventListener("message", async (event) => {
	const port = event.ports[0]!;
	const store = ChainStore.start(port);
	port.start();
	while (true) {
		try {
			await store.tick();
		} catch (error) {
			console.error("[chain] tick error:", error);
		}
	}
}, { once: true });
self.postMessage(null);
