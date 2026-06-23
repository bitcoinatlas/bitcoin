import { RocksDatabase } from "@harperfast/rocksdb-js";
import { sha256 } from "@noble/hashes/sha2";
import { delay } from "@std/async";
import { join } from "@std/path";
import { formatHash } from "~/app/frontend/utils/format.ts";
import { rawScriptPubKey, ScriptPubKey } from "~/chain/ScriptPubKey.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { COINBASE_TXID, MAX_BLOCK_SIZE, MAX_BLOCK_WEIGHT, MAX_NON_WITNESS_SIZE } from "~/constants.ts";
import { ARGS, BASE_DATA_DIR } from "~/env.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { Uint8ArrayMap } from "~/libs/collections/Uint8ArrayMap.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { Atomic, InferBatches, InferStores } from "~/libs/storage/Atomic.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { IndexStore } from "~/libs/storage/IndexStore.ts";
import { KvStore } from "~/libs/storage/KvStore.ts";
import { formatDuration } from "~/libs/formatting/mod.ts";

const GiB = 1024 ** 3;
const totalRam = Deno.systemMemoryInfo().total;

// Reserve for OS + page cache + V8 + other processes. Scale the reserve with
// total RAM (a 64 GiB box can spare proportionally more than an 8 GiB one).
const osReserve = Math.max(2 * GiB, totalRam * 0.25);

const writeBufferSize = 2 * GiB; // additive (costToCache:false)

// Block cache gets a slice of what remains, clamped to a sane band.
const available = totalRam - osReserve - writeBufferSize;
const blockCacheSize = Math.max(
	1 * GiB, // floor — below this rocksdb thrashes
	Math.min(available * 0.6, 32 * GiB), // 60% of remainder, hard ceiling
);

console.log(
	`[rocksdb] ram=${(totalRam / GiB).toFixed(1)}GiB`,
	`blockCache=${(blockCacheSize / GiB).toFixed(1)}GiB`,
	`writeBuffer=${(writeBufferSize / GiB).toFixed(1)}GiB`,
);

RocksDatabase.config({
	blockCacheSize,
	writeBufferManagerSize: writeBufferSize,
	writeBufferManagerCostToCache: false,
	writeBufferManagerAllowStall: false,
});

const rocksdb = RocksDatabase.open(join(BASE_DATA_DIR, "rocksdb"), {
	disableWAL: true,
	pessimistic: true,
	enableStats: ARGS["rocksdb-stats"],
	parallelismThreads: Math.min(10, navigator.hardwareConcurrency),
	transactionLogRetention: 0,
	bloomBitsPerKey: 10,
	ribbonFilter: true,
	transactionLogMaxSize: 0,
	keyEncoder: {
		readKey(source: any, start: number, end?: number) {
			return source.subarray(start, end);
		},
		writeKey(key: any, target: any, start: number) {
			target.set(key, start);
			return start + key.length;
		},
	},
});

if (ARGS["rocksdb-stats"]) {
	globalThis.addEventListener("unload", () => {
		try {
			logRocksStats(rocksdb);
		} catch (e) {
			console.error("[rocksdb stats] failed:", e);
		}
	});

	function logRocksStats(db: RocksDatabase): void {
		const num = (name: string) => {
			const v = db.getStat(name);
			return typeof v === "number" ? v : 0;
		};

		const cHit = num("rocksdb.block.cache.hit");
		const cMiss = num("rocksdb.block.cache.miss");
		const hitRate = cHit + cMiss ? (100 * cHit) / (cHit + cMiss) : 0;

		const bloomUseful = num("rocksdb.bloom.filter.useful");
		const bloomFullPos = num("rocksdb.bloom.filter.full.positive");

		const get = db.getStat("rocksdb.db.get.micros"); // histogram

		console.log("[rocksdb stats] -----------------------------");
		console.log(`  block cache  hit=${cHit} miss=${cMiss} hitRate=${hitRate.toFixed(2)}%`);
		console.log(`  memtable     hit=${num("rocksdb.memtable.hit")} miss=${num("rocksdb.memtable.miss")}`);
		console.log(`  sst bloom    useful=${bloomUseful} fullPositive=${bloomFullPos}`);
		if (get && typeof get === "object") {
			console.log(
				`  get.micros   p50=${get.median.toFixed(1)} p95=${get.percentile95.toFixed(1)} ` +
					`p99=${get.percentile99.toFixed(1)} max=${get.max.toFixed(0)} count=${get.count}`,
			);
		}
		console.log(db.getStats()); // full curated dump for everything else
	}
}

const atomic = Atomic.open({
	path: join(BASE_DATA_DIR, "atomic"),
	stores: {
		header: ArrayStore.open({
			path: join(BASE_DATA_DIR, "header"),
			codec: StoredBlockHeader,
			diskItemsPerChunk: 1_000_000,
			memoryItemsPerChunk: 1_000_000,
		}),
		block: ArrayStore.open({
			path: join(BASE_DATA_DIR, "block"),
			codec: StoredPointer,
			diskItemsPerChunk: 1_000_000,
			memoryItemsPerChunk: 1_000_000,
		}),
		tx: BlobStore.open({
			path: join(BASE_DATA_DIR, "tx"),
			maxDiskChunkSize: 1 * 1000 * 1000 * 1000,
			maxMemoryChunkSize: MAX_BLOCK_SIZE,
		}),
		txid: KvStore.open({
			rocksdb,
			prefix: new Uint8Array([0]),
			key: Bytes32,
			value: StoredPointer,
		}),
		pubkey: KvStore.open({
			rocksdb,
			prefix: new Uint8Array([1]),
			key: Bytes32,
			value: StoredPointer,
		}),
		spender: IndexStore.open({
			path: join(BASE_DATA_DIR, "spender"),
			codec: StoredPointer,
			itemsPerChunk: 1_000_000,
		}),
	},
});

atomic.recover();

export class ChainStore {
	public readonly blockHashToHeightMap: Uint8ArrayMap<number>;
	public readonly atomic = atomic;

	// Reused across every _appendTxs call/tx so encodeWithOffsets never
	// allocates per-tx. Sized to the max a single serialized tx can be
	// (bounded by block size/weight); append() copies the live prefix out.
	private readonly txScratch = new Uint8Array(MAX_BLOCK_SIZE);

	private p2pChannel: MessagePort;
	private p2pMessageQueue: Queue<{ type: string; data: any }>;

	private constructor(p2pChannel: MessagePort, initialHeaders: WireBlockHeader[]) {
		this.p2pChannel = p2pChannel;
		this.p2pMessageQueue = new Queue(1000);
		this.blockHashToHeightMap = new Uint8ArrayMap<number>(Math.max(256, initialHeaders.length * 2));
		for (let i = 0; i < initialHeaders.length; i++) {
			this.blockHashToHeightMap.set(initialHeaders[i]!.hash(), i);
		}
	}

	static start(p2pChannel: MessagePort): ChainStore {
		const headers = atomic.stores.header.slice(0, atomic.stores.header.length());
		console.log(`[chain] loaded ${headers.length} headers from disk`);
		const self = new ChainStore(p2pChannel, headers);

		p2pChannel.addEventListener("message", (event) => self.p2pMessageQueue.enqueue(event.data));
		const startHeaders = atomic.stores.header.slice(0, atomic.stores.header.length());
		console.log(`[chain] handing ${startHeaders.length} headers to worker`);
		const startData = WireBlockHeaders.encode(startHeaders);
		p2pChannel.postMessage({ type: "seek", data: atomic.stores.block.length() - 1 });
		p2pChannel.postMessage({ type: "start", data: startData }, [startData.buffer]);
		return self;
	}

	private startTime: number | undefined;
	private totalTxs: number = 0;
	private totalBlocks: number = 0;
	private totalSize: number = 0;
	async tick(): Promise<void> {
		const message = this.p2pMessageQueue.dequeue();
		if (!message) {
			await delay(0);
			return;
		}
		if (message.type === "blocks") {
			if (this.startTime) {
				const passed = performance.now() - this.startTime;
				const passedSeconds = passed / 1000;
				const speedTxs = this.totalTxs / passedSeconds;
				const speedSize = (this.totalSize / 1024 / 1024) / passedSeconds;
				const speedBlocks = this.totalBlocks / passedSeconds;
				console.log(
					`[chain] sustained speed`,
					`${speedBlocks.toFixed(1)}blocks/s`,
					`${speedTxs.toFixed(0)}txs/s`,
					`${speedSize.toFixed(2)}MiB/s`,
					`time=${formatDuration(passed)}`,
				);
			}
			this.startTime ??= performance.now();
			const buffer = message.data as Uint8Array;
			console.log(`[chain] new chunk to consume size=${buffer.length}`);
			let offset = 0;
			let blocks = 0;
			while (offset < buffer.length) {
				const [txs, size] = StoredTxs.decode(buffer.subarray(offset));
				offset += size;
				blocks++;
				this.appendTxs(txs, atomic.stores.block.length());
				if (this.startTime) {
					this.totalTxs += txs.length;
					this.totalSize += size;
					this.totalBlocks++;
				}
			}
			this.requestFlush();
			this.p2pChannel.postMessage({ type: "consume" });
			console.log(`[chain] consumed blocks count=${blocks} bytes=${offset} height=${atomic.stores.block.length() - 1}`);

			return;
		}

		if (message.type === "headers") {
			const headers = WireBlockHeaders.decodeValue(message.data);
			const { height } = this.pushHeaders(headers);
			console.log(`[chain] tick headers height=${height} count=${headers.length}`);
			this.requestFlush();
			return;
		}

		if (message.type === "reorg") {
			console.log(`[chain] tick reorg keepHeight=${message.data}`);
			// truncate() rejects staged/frozen data, so flush to disk first
			while (atomic.busy) await delay(1);
			atomic.flush();
			this.reorg(message.data);
			this.requestFlush();
			return;
		}
	}

	private needFlush = false;
	private requestFlush() {
		if (atomic.busy) {
			this.needFlush = true;
			return;
		}
		atomic.flush(); /* .then(() => {
			if (!this.needFlush) return;
			this.needFlush = false;
			this.requestFlush();
		}); */
	}

	private pushHeaders(headers: WireBlockHeader[], batches?: InferBatches<typeof atomic, "header">): { height: number } {
		const batch = batches ?? atomic.batch(["header"]);

		const op = () => {
			for (const header of headers) {
				const height = batch.header.push(header);
				this.blockHashToHeightMap.set(header.hash(), height);
			}
		};

		if (batches) {
			op();
			return { height: batch.header.length() - 1 };
		}

		try {
			op();
			batch.header.apply();
			return { height: batch.header.length() - 1 };
		} catch (reason) {
			batch.header.discard();
			console.error("Failed to append block header:", reason);
			Deno.exit(1);
		}
	}

	private _rawScriptPubKeyBuffer = new Uint8Array(new ArrayBuffer(0, { maxByteLength: MAX_NON_WITNESS_SIZE }));
	// input prevOut txids (skip raw-coinbase; same-block ones resolve locally in phase 2)
	private _txidKeys = new FastUint8ArrayMap<number>(64);
	// output scriptPubKey hashes
	private _pubkeyKeys = new FastUint8ArrayMap<number>(64);
	private _pubkeyHashes: (Uint8Array | null)[] = []; // [t][i] -> hash or null, computed once
	private appendTxs(
		txs: StoredTx[],
		height: number,
		batches?: InferBatches<typeof atomic>,
	): { pointer: StoredPointer } {
		const batch = batches ?? atomic.batch();

		const op = () => {
			const txCountBytes = StoredTxs.counter.encode(txs.length);
			const blockPointer = batch.tx.append(txCountBytes);

			// --- PHASE 1: prefetch all RocksDB reads up front, in parallel ---
			// We don't know same-block pointers yet, so we just prefetch *whether* each key
			// exists in RocksDB. Same-block cases are handled by local maps in phase 2.

			this._txidKeys.clear();
			this._pubkeyKeys.clear();
			this._pubkeyHashes.length = 0;
			for (let t = 0; t < txs.length; t++) {
				const tx = txs[t]!;
				for (const input of tx.inputs) {
					if (input.prevOut.txId.kind === "raw") this._txidKeys.set(input.prevOut.txId.value, 1);
				}
				for (const output of tx.outputs) {
					if (output.scriptPubKey.kind === "pointer") {
						this._pubkeyHashes.push(null);
						continue;
					}
					// computed ONCE here, reused everywhere below
					const hash = sha256(rawScriptPubKey(output.scriptPubKey, this._rawScriptPubKeyBuffer));
					this._pubkeyHashes.push(hash);
					this._pubkeyKeys.set(hash, 1);
				}
			}

			const txidPrefetch = new FastUint8ArrayMap<StoredPointer>(this._txidKeys.size());
			const pubkeyPrefetch = new FastUint8ArrayMap<StoredPointer>(this._pubkeyKeys.size());
			for (const id of this._txidKeys.keys()) {
				const p = batch.txid.get(id);
				if (p !== undefined) txidPrefetch.set(id, p);
			}
			for (const h of this._pubkeyKeys.keys()) {
				const p = batch.pubkey.get(h);
				if (p !== undefined) pubkeyPrefetch.set(h, p);
			}

			// --- PHASE 2: sequential, no RocksDB ---
			const blockTxIds = new FastUint8ArrayMap<number>(txs.length * 2); // same-block txid -> pointer
			const blockPubkeys = new FastUint8ArrayMap<StoredPointer>(64); // same-block hash -> pointer
			let pubKeyHashesOffset = 0;
			let offset = txCountBytes.length;
			for (let t = 0; t < txs.length; t++) {
				const tx = txs[t]!;
				const txPointer = blockPointer + offset;
				tx.spender = batch.spender.length();
				batch.txid.set(tx.txId, txPointer);
				blockTxIds.set(tx.txId, txPointer);

				// inputs: local block map first, then prefetched
				for (let i = 0; i < tx.inputs.length; i++) {
					const input = tx.inputs[i]!;
					if (input.prevOut.txId.kind !== "raw") continue;
					const id = input.prevOut.txId.value;
					const pointer = blockTxIds.get(id) ?? txidPrefetch.get(id);
					if (pointer === undefined) {
						console.error(`[appendTxs] unresolved prevOut height=${height} tx=${t} vin=${i}`);
						Deno.exit(1);
					}
					input.prevOut.txId = { kind: "pointer", value: pointer };
				}

				// scriptPubKey reuse: local block map first, then prefetched
				for (let i = 0; i < tx.outputs.length; i++) {
					const output = tx.outputs[i]!;
					if (output.scriptPubKey.kind === "pointer") continue;
					const hash = this._pubkeyHashes[pubKeyHashesOffset + i]!;
					const existing = blockPubkeys.get(hash) ?? pubkeyPrefetch.get(hash);
					if (existing !== undefined) output.scriptPubKey = { kind: "pointer", value: existing };
				}

				// size is computed AFTER resolution above — raw→pointer changes the byte length
				const size = StoredTx.size(tx);
				const offsets = StoredTx.encodeWithOffsets(tx, this.txScratch, 0);
				const written = batch.tx.append(this.txScratch.subarray(0, size));
				if (written !== txPointer) {
					throw new Error(`[appendTxs] pointer drift: append=${written} txPointer=${txPointer}`);
				}

				// pubkey index writes: reuse the same hashes, dedup via local + prefetch
				for (let i = 0; i < tx.outputs.length; i++) {
					const output = tx.outputs[i]!;
					batch.spender.push(0);
					if (output.scriptPubKey.kind === "pointer") continue;
					const hash = this._pubkeyHashes[pubKeyHashesOffset + i]!;
					if (blockPubkeys.get(hash) === undefined && pubkeyPrefetch.get(hash) === undefined) {
						const ptr = txPointer + offsets.outputs[i]!;
						batch.pubkey.set(hash, ptr);
						blockPubkeys.set(hash, ptr); // so a later same-block output reuses it
					}
				}

				for (let i = 0; i < tx.inputs.length; i++) {
					const input = tx.inputs[i]!;
					if (input.prevOut.txId.kind !== "pointer") continue;
					const txSpenderOffset = batch.tx.get(input.prevOut.txId.value + Bytes32.stride.size, U40);
					const spenderIndex = txSpenderOffset + input.prevOut.vout;
					const spender = batch.spender.get(spenderIndex);
					if (spender && spender > 0) {
						const txid = batch.tx.get(input.prevOut.txId.value, Bytes32);
						throw new Error(`Output ${formatHash(txid)}:${input.prevOut.vout} is already spent.`);
					}
					batch.spender.set(spenderIndex, txPointer);
				}

				offset += size;
				pubKeyHashesOffset = tx.outputs.length;
			}

			const currentLength = batch.block.length();
			if (currentLength !== height) throw new Error(`Unexpected length=${height}, got ${currentLength}`);
			batch.block.push(blockPointer);
			return blockPointer;
		};

		if (batches) {
			const blockPointer = op();
			return { pointer: blockPointer };
		}

		try {
			const blockPointer = op();
			batch.header.apply();
			batch.block.apply();
			batch.spender.apply();
			batch.tx.apply();
			batch.txid.apply();
			batch.pubkey.apply();

			return { pointer: blockPointer };
		} catch (reason) {
			batch.header.discard();
			batch.block.discard();
			batch.spender.discard();
			batch.tx.discard();
			batch.txid.discard();
			batch.pubkey.discard();
			console.error("Failed to append txs:", reason);
			Deno.exit(1);
		}
	}

	private reorg(keepHeight: number): void {
		const blocksHeight = atomic.stores.block.length() - 1;
		console.log(`[chain] reorg: keepHeight=${keepHeight} currentTip=${blocksHeight}`);
		if (keepHeight >= blocksHeight) return; // nothing to undo

		// byte offset in the tx blob where the orphaned suffix begins
		const cutOffset = atomic.stores.block.get(keepHeight + 1);
		if (cutOffset === undefined) throw new Error(`reorg: no block pointer at ${keepHeight + 1}`);

		// spender array cut = the spender base of the first tx of the first orphaned block
		const firstOrphanTxs = this.getTxsByBlockHeight(keepHeight + 1);
		if (!firstOrphanTxs?.length) throw new Error(`reorg: no txs at ${keepHeight + 1}`);
		const spenderCut = firstOrphanTxs[0]!.spender;
		console.log(`[chain] reorg: cutOffset=${cutOffset} spenderCut=${spenderCut}`);

		const batch = atomic.batch();

		// tombstone orphaned txid / pubkey entries (pointer >= cutOffset only)
		for (let h = keepHeight + 1; h <= blocksHeight; h++) {
			const txs = this.getTxsByBlockHeight(h);
			if (!txs) continue;
			for (const tx of txs) {
				const existingTxid = batch.txid.get(tx.txId);
				if (existingTxid !== undefined && existingTxid >= cutOffset) {
					batch.txid.delete(tx.txId); // assumed KvStore.delete
				}
				for (const output of tx.outputs) {
					if (output.scriptPubKey.kind === "pointer") continue;
					const hash = sha256(rawScriptPubKey(output.scriptPubKey, this._rawScriptPubKeyBuffer));
					const existingPub = batch.pubkey.get(hash);
					if (existingPub !== undefined && existingPub >= cutOffset) {
						batch.pubkey.delete(hash); // assumed KvStore.delete
					}
				}
			}
			const header = atomic.stores.header.get(h);
			if (header) this.blockHashToHeightMap.delete(header.hash());
		}

		batch.txid.apply();
		batch.pubkey.apply();

		// truncate the array/blob stores down to the surviving prefix
		atomic.stores.header.truncate(keepHeight + 1);
		atomic.stores.block.truncate(keepHeight + 1);
		atomic.stores.spender.truncate(spenderCut);
		atomic.stores.tx.truncate(cutOffset);
		console.log(`[chain] reorg complete: stores truncated to height=${keepHeight}`);
	}

	getHeaderByHeight(height: number): StoredBlockHeader | undefined {
		const header = atomic.stores.header.get(height);
		if (!header) return undefined;
		return header;
	}

	getHeaderByRange(
		from: number,
		to: number,
	): Array<{ height: number; header: WireBlockHeader }> {
		const headers = atomic.stores.header.slice(from, to + 1);
		return headers.map((header, i) => ({ height: from + i, header: header }));
	}

	getHeaderByHash(hash: Uint8Array): StoredBlockHeader | undefined {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return this.getHeaderByHeight(height);
	}

	getTxByPointer(pointer: StoredPointer): StoredTx {
		const storedTx = atomic.stores.tx.get(pointer, StoredTx, { readAheadSize: 400_000 });
		return storedTx;
	}

	getTxById(txId: Uint8Array): StoredTx | undefined {
		const pointer = atomic.stores.txid.get(txId);
		if (pointer === undefined) return undefined;
		return this.getTxByPointer(pointer);
	}

	getTxsByBlockPointer(pointer: StoredPointer): StoredTx[] | undefined {
		const storedTxs = atomic.stores.tx.get(pointer, StoredTxs, { readAheadSize: MAX_BLOCK_WEIGHT });
		return storedTxs;
	}

	getTxsByBlockHeight(height: number): StoredTx[] | undefined {
		const pointer = height === 0 ? 0 : atomic.stores.block.get(height);
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

	getBlockPointerByHeight(height: number): StoredPointer | undefined {
		return atomic.stores.block.get(height);
	}

	getBlockPointerByHash(hash: Uint8Array): StoredPointer | undefined {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return this.getBlockPointerByHeight(height);
	}

	getChainTip(): { height: number; header: StoredBlockHeader } | undefined {
		const height = atomic.stores.header.length() - 1;
		if (height < 0) return undefined;
		const header = this.getHeaderByHeight(height);
		if (!header) return undefined;
		return { height, header };
	}

	getTxOutputByPointer(
		pointer: number,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): StoredTxOutput {
		return (batches ?? atomic.stores).tx.get(pointer, StoredTxOutput);
	}

	getScriptPubKey(
		output: StoredTxOutput,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): ScriptPubKey {
		if (output.scriptPubKey.kind === "pointer") {
			const resolved = this.getTxOutputByPointer(output.scriptPubKey.value, batches);
			if (resolved.scriptPubKey.kind === "pointer") {
				throw new Error([
					`scriptPubKey resolution failed: pointer ${output.scriptPubKey.value} points to another pointer.`,
					`Expected direct ScriptPubKey at that offset.`,
				].join(" "));
			}
			return resolved.scriptPubKey;
		} else {
			return output.scriptPubKey;
		}
	}

	getPrevOutTxId(input: StoredTxInput): Uint8Array {
		const txId = input.prevOut.txId;
		const { kind, value } = txId;
		if (kind === "raw") {
			return value;
		}

		if (kind === "pointer") {
			return this.getTxByPointer(value).txId;
		}

		if (kind === "coinbase") {
			return COINBASE_TXID;
		}

		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}
}
