import { delay } from "@std/async";
import { concat } from "@std/bytes";
import { atomic } from "~/chain/atomic.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { StoredPrevOutTxId } from "~/codec/stored/StoredPrevOutTxId.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { COINBASE_TXID, MAX_BLOCK_WEIGHT, SECOND } from "~/constants.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { Uint8ArrayMap } from "~/libs/collections/Uint8ArrayMap.ts";
import { MessagePortLike } from "~/libs/message/mod.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { PARALLELISM } from "~/env.ts";

/**
 * One block as emitted by consume.worker's `process` stage: encoded StoredTxs
 * plus the metadata the commit thread needs to finalize it. All offsets are
 * relative to the start of `buffer`. Mirrors the type in consume.worker.ts —
 * kept in sync by hand rather than importing (the worker module has top-level
 * side effects and must not be imported into the main thread).
 */
type EncodedBlock = {
	buffer: Uint8Array;
	txIds: Uint8Array;
	txOffsets: Uint32Array;
	patchOffsets: Uint32Array;
	patchTxids: Uint8Array;
};

/** consume.worker `init` output: unknown scriptPubKeys, pre-hashed + pre-encoded. */
type InitResult = {
	/** hash of each unknown pubkey, packed 32 bytes each (for cross-worker dedup). */
	hashes: Uint8Array;
	/** StoredScriptPubKey bytes of each unknown pubkey, back-to-back. */
	encoded: Uint8Array;
	/** encoded length of each unknown pubkey; slices `encoded`. */
	lengths: Uint32Array;
};

function hex(bytes: Uint8Array): string {
	let s = "";
	for (const b of bytes) s += b.toString(16).padStart(2, "0");
	return s;
}

/** Human-readable ETA. `—` when not yet computable (no rate, or already at tip). */
function formatEta(seconds: number): string {
	if (!isFinite(seconds) || seconds <= 0) return "—";
	const h = Math.floor(seconds / 3600);
	const m = Math.floor((seconds % 3600) / 60);
	if (h >= 24) return `${Math.floor(h / 24)}d${h % 24}h`;
	return h > 0 ? `${h}h${m}m` : `${m}m`;
}

export class ChainStore {
	public readonly blockHashToHeightMap: Uint8ArrayMap<number>;
	public readonly atomic = atomic;

	private p2pChannel: MessagePortLike;
	private p2pMessageQueue: Queue<{ type: string; data: any }>;

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
	// [LOG] batch counter.
	private round = 0;
	private startTime = performance.now();
	private totalTxs = 0;
	// [LOG] instantaneous-rate state. lastRoundTime bounds the per-round Δt; the
	// rolling window smooths the "current" rate so it tracks the density ramp
	// without twitching on every round (single-round Δ is too jittery).
	private lastRoundTime = performance.now();
	private readonly rateWindow: Array<{ txs: number; blocks: number; ms: number }> = [];
	private static readonly RATE_WINDOW_ROUNDS = 12;

	private constructor(p2pChannel: MessagePortLike, initialHeaders: WireBlockHeader[]) {
		this.p2pChannel = p2pChannel;
		this.p2pMessageQueue = new Queue(1000);
		this.blockHashToHeightMap = new Uint8ArrayMap<number>(Math.max(256, initialHeaders.length * 2));
		for (let i = 0; i < initialHeaders.length; i++) {
			this.blockHashToHeightMap.set(initialHeaders[i]!.hash(), i);
		}

		this.consumers = new Array(PARALLELISM);
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
					if (++this.readyCount === this.consumers.length) {
						this.consumersReady.resolve();
					}
					return;
				}
				if (stage !== "error") return;
				const e = event.data as { phase: string; message: string; stack?: string };
				console.error(`[chain] consumer-${i} threw in ${e.phase}: ${e.message}\n${e.stack ?? ""}`);
				Deno.exit(1);
			});
			this.consumers[i] = worker;
		}
	}

	static start(p2pChannel: MessagePortLike): ChainStore {
		const headers = atomic.stores.headers.slice(0, atomic.stores.headers.length());
		const self = new ChainStore(p2pChannel, headers);

		p2pChannel.addEventListener("message", (event) => self.p2pMessageQueue.enqueue(event.data));
		const startHeaders = atomic.stores.headers.slice(0, atomic.stores.headers.length());
		const startData = WireBlockHeaders.encode(startHeaders);
		p2pChannel.postMessage({ type: "seek", data: atomic.stores.blocks.length() - 1 });
		p2pChannel.postMessage({ type: "start", data: startData }, [startData.buffer]);
		return self;
	}

	getHeaderByHeight(height: number): StoredBlockHeader | undefined {
		const header = atomic.stores.headers.get(height);
		if (!header) return undefined;
		return header;
	}

	getHeaderByRange(
		from: number,
		to: number,
	): Array<{ height: number; header: WireBlockHeader }> {
		const headers = atomic.stores.headers.slice(from, to + 1);
		return headers.map((header, i) => ({ height: from + i, header: header }));
	}

	getHeaderByHash(hash: Uint8Array): StoredBlockHeader | undefined {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return this.getHeaderByHeight(height);
	}

	getTxByPointer(pointer: StoredTxPointer): StoredTx {
		const storedTx = atomic.stores.txs.get(pointer, StoredTx, { readAheadSize: 400_000 });
		return storedTx;
	}

	getTxById(txId: Uint8Array): StoredTx | undefined {
		const pointer = atomic.stores.txid.get(txId);
		if (pointer === undefined) return undefined;
		return this.getTxByPointer(pointer);
	}

	getTxsByBlockPointer(pointer: StoredTxPointer): StoredTx[] | undefined {
		const storedTxs = atomic.stores.txs.get(pointer, StoredTxs, { readAheadSize: MAX_BLOCK_WEIGHT });
		return storedTxs;
	}

	getTxsByBlockHeight(height: number): StoredTx[] | undefined {
		const pointer = height === 0 ? 0 : atomic.stores.blocks.get(height);
		if (pointer === undefined) return undefined;
		return this.getTxsByBlockPointer(pointer);
	}

	getTxsByBlockHash(hash: Uint8Array): StoredTx[] | undefined {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return this.getTxsByBlockHeight(height);
	}

	getHeightByHash(hash: Uint8Array): number | undefined {
		return this.blockHashToHeightMap.get(hash);
	}

	getHashByHeight(height: number): Uint8Array | undefined {
		const header = this.getHeaderByHeight(height);
		if (!header) return undefined;
		return header.hash();
	}

	getBlockPointerByHeight(height: number): StoredTxPointer | undefined {
		return atomic.stores.blocks.get(height);
	}

	getBlockPointerByHash(hash: Uint8Array): StoredTxPointer | undefined {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return this.getBlockPointerByHeight(height);
	}

	getChainTip(): { height: number; header: StoredBlockHeader } | undefined {
		const height = atomic.stores.headers.length() - 1;
		if (height < 0) return undefined;
		const header = this.getHeaderByHeight(height);
		if (!header) return undefined;
		return { height, header };
	}

	getPrevOutTxId(input: StoredTxInput): Uint8Array {
		const txId = input.prevOut.txId;
		const { kind, value } = txId;

		if (kind === "pointer") {
			return this.getTxByPointer(value).txId;
		}

		if (kind === "coinbase") {
			return COINBASE_TXID;
		}

		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}

	async tick(): Promise<void> {
		const message = this.p2pMessageQueue.dequeue();
		if (!message) {
			// Nothing pending. If chunks are waiting but we never reached a full
			// batch (tail of IBD, or p2p idle at the tip), flush the partial now so
			// it doesn't stall. During fast IBD the queue reaches batchSize before
			// it ever empties, so this only fires when p2p genuinely has nothing.
			if (this.chunkQueue.size() > 0) await this.runBatch();
			else await delay(0);
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
			const height = message.data as number;
			this.handleReorgMesage(height);
			return;
		}
	}

	private handleReorgMesage(_keepHeight: number): void {
		throw new Error("Not Implemented");
	}

	private async handleHeadersMessage(headers: WireBlockHeaders) {
		try {
			let height = atomic.stores.headers.length();
			await atomic.trx((stores) => {
				for (const header of headers) {
					height = stores.headers.push(header);
					this.blockHashToHeightMap.set(header.hash(), height);
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

	/** Stage 1: worker decodes its chunk, returns unknown scriptPubKeys. */
	private initWorker(worker: Worker, chunk: Uint8Array, id: number): Promise<InitResult> {
		return new Promise((resolve, reject) => {
			const onMessage = (event: MessageEvent) => {
				const data = event.data as {
					stage: string;
					hashes?: Uint8Array;
					encoded?: Uint8Array;
					lengths?: Uint32Array;
					message?: string;
				};
				if (data.stage === "error") {
					worker.removeEventListener("message", onMessage);
					reject(new Error(`consumer-${id} init: ${data.message}`));
					return;
				}
				if (data.stage !== "init-done") return;
				worker.removeEventListener("message", onMessage);
				resolve({ hashes: data.hashes!, encoded: data.encoded!, lengths: data.lengths! });
			};
			worker.addEventListener("message", onMessage);
			worker.postMessage({ stage: "init", data: chunk }, [chunk.buffer]);
		});
	}

	/** Stage 2: worker encodes blocks with assigned pubkey pointers. */
	private processWorker(worker: Worker, pointers: BigUint64Array, id: number): Promise<EncodedBlock[]> {
		return new Promise((resolve, reject) => {
			const onMessage = (event: MessageEvent) => {
				const data = event.data as { stage: string; blocks?: EncodedBlock[]; message?: string };
				if (data.stage === "error") {
					worker.removeEventListener("message", onMessage);
					reject(new Error(`consumer-${id} process: ${data.message}`));
					return;
				}
				if (data.stage !== "process-done") return;
				worker.removeEventListener("message", onMessage);
				resolve(data.blocks!);
			};
			worker.addEventListener("message", onMessage);
			worker.postMessage({ stage: "process", data: pointers }, [pointers.buffer]);
		});
	}

	/**
	 * Process one batch of chunks: Parallel.For (init | process) with a serial
	 * join at commit. init and process run OUTSIDE the write transaction — pubkey
	 * pointers are assigned as prospective blob offsets (replayed and asserted at
	 * commit), so the trx is short and synchronous and worker reads never race an
	 * open write. Barrier state is entirely local to this call.
	 */
	private async runBatch(): Promise<void> {
		// Never post work to workers that haven't loaded yet (dropped-message hang).
		await this.consumersReady.promise;
		const batch = this.drainBatch();
		if (batch.length === 0) return;

		const round = ++this.round;

		// 1) init all chunks in parallel; each returns its unknown scriptPubKeys.
		const pubkeysPerWorker = await Promise.all(
			batch.map((chunk, i) => this.initWorker(this.consumers[i]!, chunk, i)),
		);

		// 2) Assign pubkey pointers WITHOUT touching disk, and build ONE blob of the
		//    genuinely-new pubkeys (deduped across workers by hash) for a single
		//    append. Workers already hashed + encoded them, so this loop only dedups
		//    and copies subarrays — no sha256, no encode on the chain thread.
		const cache = new FastUint8ArrayMap<number>();
		const newHashes: Uint8Array[] = []; // parallel arrays for the pubkey index writes
		const newPointers: number[] = [];
		const blobParts: Uint8Array[] = []; // encoded subarrays of the new uniques
		const pubkeyBase = atomic.stores.pubkeys.size();
		let pubkeyCursor = pubkeyBase;
		const pointersPerWorker: BigUint64Array[] = new Array(batch.length);

		for (let i = 0; i < batch.length; i++) {
			const { hashes, encoded, lengths } = pubkeysPerWorker[i]!;
			const n = lengths.length;
			const pointers = new BigUint64Array(n);
			let encOffset = 0;
			for (let j = 0; j < n; j++) {
				const len = lengths[j]!;
				const hash = hashes.subarray(j * 32, j * 32 + 32);
				let ptr = cache.get(hash);
				if (ptr === undefined) {
					ptr = atomic.stores.pubkey.get(hash);
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
		const pubkeyBlob = concat(blobParts);

		// 3) process all chunks in parallel; each returns encoded blocks + patch meta.
		const blocksPerWorker = await Promise.all(
			batch.map((_, i) => this.processWorker(this.consumers[i]!, pointersPerWorker[i]!, i)),
		);

		// 4) Prep blocks OUTSIDE the trx: assign block/tx pointers, register txIds so
		//    in-batch prevOuts resolve, patch them, concatenate every block into ONE
		//    blob. The trx below is then pure disk writes (batched appends + puts).
		const batchTxid = new FastUint8ArrayMap<number>();
		const txBase = atomic.stores.txs.size();
		const blockBases: number[] = []; // abs offset of each block, for blocks.pushMany
		let committedBlocks = 0;
		let committedTxs = 0;
		let blockCursor = txBase;

		for (let i = 0; i < blocksPerWorker.length; i++) {
			for (const block of blocksPerWorker[i]!) {
				blockBases.push(blockCursor);
				const txCount = block.txOffsets.length;
				for (let k = 0; k < txCount; k++) {
					const txId = block.txIds.subarray(k * 32, k * 32 + 32);
					batchTxid.set(txId, blockCursor + block.txOffsets[k]!);
				}
				blockCursor += block.buffer.length;
				committedBlocks++;
				committedTxs += txCount;
			}
		}
		let patched = 0;
		for (let i = 0; i < blocksPerWorker.length; i++) {
			for (const block of blocksPerWorker[i]!) {
				const patchCount = block.patchOffsets.length;
				for (let p = 0; p < patchCount; p++) {
					const prevTxId = block.patchTxids.subarray(p * 32, p * 32 + 32);
					const pointer = batchTxid.get(prevTxId) ?? atomic.stores.txid.get(prevTxId);
					if (pointer === undefined) throw new Error(`unresolved prevOut at commit txid=${hex(prevTxId)}`);
					StoredPrevOutTxId.patchPointer(block.buffer, block.patchOffsets[p]!, pointer);
					patched++;
				}
			}
		}
		const blockParts: Uint8Array[] = [];
		for (let i = 0; i < blocksPerWorker.length; i++) {
			for (const block of blocksPerWorker[i]!) blockParts.push(block.buffer);
		}
		const blockBlob = concat(blockParts);

		// 5) Commit: pure batched writes.
		await atomic.trx((stores, trx) => {
			if (pubkeyBlob.length > 0) {
				const off = stores.pubkeys.append(pubkeyBlob);
				if (off !== pubkeyBase) throw new Error(`pubkey blob offset mismatch: expected ${pubkeyBase} got ${off}`);
			}
			const pubkeyEntries: [Uint8Array, number][] = new Array(newHashes.length);
			for (let u = 0; u < newHashes.length; u++) pubkeyEntries[u] = [newHashes[u]!, newPointers[u]!];
			stores.pubkey.setMany(pubkeyEntries, trx);
			// txid index: pointer = block base + tx offset within its block.
			const txidEntries: [txid: Uint8Array, pointer: number][] = new Array(committedTxs);
			let te = 0;
			let bc = txBase;
			for (let i = 0; i < blocksPerWorker.length; i++) {
				for (const block of blocksPerWorker[i]!) {
					const txCount = block.txOffsets.length;
					for (let k = 0; k < txCount; k++) {
						const txId = block.txIds.subarray(k * 32, k * 32 + 32);
						txidEntries[te++] = [txId, bc + block.txOffsets[k]!];
					}
					bc += block.buffer.length;
				}
			}
			stores.txid.setMany(txidEntries, trx);
			if (blockBlob.length > 0) {
				const off = stores.txs.append(blockBlob);
				if (off !== txBase) throw new Error(`txs blob offset mismatch: expected ${txBase} got ${off}`);
			}
			stores.blocks.pushMany(blockBases);
		});

		// Throughput. Three numbers:
		//   overall  — totalTxs / totalElapsed. Lifetime mean. Drifts vs reality as
		//              block density changes (it's what made the old `rate` field read
		//              ~2x high — it carried genesis-era ballast). Keep it for context,
		//              but don't extrapolate from it.
		//   current  — Δtx/Δt over the last N rounds. Tracks the density ramp in near
		//              real time; single-round Δ is too jittery, a short window isn't.
		//   ETA      — remainingBlocks / current blocks-per-sec. Block-based on purpose:
		//              tx counts of unreached blocks are unknown in-process, so a
		//              tx-based ETA can't be computed here. OPTIMISTIC across the ramp —
		//              blocks/s falls as you climb into denser years, so treat this as a
		//              lower bound (esp. 2023-24). Assumes headers are fully synced ahead
		//              (true during IBD once `top` is stable), so headers.length()-1 is
		//              the real target.
		this.totalTxs += committedTxs;
		const now = performance.now();
		const roundMs = now - this.lastRoundTime;
		this.lastRoundTime = now;

		this.rateWindow.push({ txs: committedTxs, blocks: committedBlocks, ms: roundMs });
		if (this.rateWindow.length > ChainStore.RATE_WINDOW_ROUNDS) this.rateWindow.shift();
		let winTxs = 0, winBlocks = 0, winMs = 0;
		for (const s of this.rateWindow) {
			winTxs += s.txs;
			winBlocks += s.blocks;
			winMs += s.ms;
		}

		const overallSec = (now - this.startTime) / SECOND;
		const overallRate = overallSec > 0 ? (this.totalTxs / overallSec) | 0 : 0;
		const currentRate = winMs > 0 ? ((winTxs / winMs) * SECOND) | 0 : 0;
		const blocksPerSec = winMs > 0 ? (winBlocks / winMs) * SECOND : 0;

		const targetHeight = atomic.stores.headers.length() - 1;
		const currentHeight = atomic.stores.blocks.length() - 1;
		const remainingBlocks = Math.max(0, targetHeight - currentHeight);
		const etaSec = blocksPerSec > 0 ? remainingBlocks / blocksPerSec : 0;

		console.log(
			`[chain] round ${round} | blocks=${committedBlocks} txs=${committedTxs} ` +
				`height=${currentHeight}/${targetHeight} | ` +
				`overall ${overallRate} tx/s · current ${currentRate} tx/s · ${blocksPerSec.toFixed(1)} blk/s | ` +
				`remaining=${remainingBlocks} ETA ${formatEta(etaSec)}`,
		);

		// 6) let p2p refill: one consume per chunk we drained.
		for (let i = 0; i < batch.length; i++) this.p2pChannel.postMessage({ type: "consume" });
	}
}
