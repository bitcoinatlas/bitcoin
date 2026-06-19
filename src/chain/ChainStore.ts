import { RocksDatabase } from "@harperfast/rocksdb-js";
import { sha256 } from "@noble/hashes/sha2";
import { join } from "@std/path";
import { formatHash } from "~/app/frontend/utils/format.ts";
import { rawScriptPubKey, ScriptPubKey } from "~/chain/ScriptPubKey.ts";
import { GENESIS_BLOCK } from "~/chain/genesis.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { WireBlock } from "~/codec/wire/WireBlock.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { BASE_DATA_DIR } from "~/env.ts";
import { COINBASE_TXID, MAX_BLOCK_SIZE, MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { ArrayStore } from "~/storage/ArrayStore.ts";
import { Atomic, InferBatches, InferStores } from "~/storage/Atomic.ts";
import { BlobStore } from "~/storage/BlobStore.ts";
import { IndexStore } from "~/storage/IndexStore.ts";
import { KvStore } from "~/storage/KvStore.ts";
import { Uint8ArrayMap } from "~/libs/collections/Uint8ArrayMap.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";

RocksDatabase.config({
	blockCacheSize: 4 * 1024 * 1024 * 1024,
	writeBufferManagerSize: 2 * 1024 * 1024 * 1024, // was 512MB
	writeBufferManagerCostToCache: false, // was true — stop cache thrash
	writeBufferManagerAllowStall: false, // keep memtables soft, no hard write wall
});

const rocksdb = RocksDatabase.open(join(BASE_DATA_DIR, "rocksdb"), {
	disableWAL: true,
	pessimistic: true,
	parallelismThreads: Math.min(10, navigator.hardwareConcurrency),
	transactionLogRetention: 0,
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

const atomic = await Atomic.open({
	path: join(BASE_DATA_DIR, "atomic"),
	stores: {
		header: await ArrayStore.open({
			path: join(BASE_DATA_DIR, "header"),
			codec: StoredBlockHeader,
			diskItemsPerChunk: 1_000_000,
			memoryItemsPerChunk: 1_000_000,
		}),
		block: await ArrayStore.open({
			path: join(BASE_DATA_DIR, "block"),
			codec: StoredPointer,
			diskItemsPerChunk: 1_000_000,
			memoryItemsPerChunk: 1_000_000,
		}),
		tx: await BlobStore.open({
			path: join(BASE_DATA_DIR, "tx"),
			maxDiskChunkSize: 1 * 1000 * 1000 * 1000,
			maxMemoryChunkSize: MAX_BLOCK_SIZE,
		}),
		txid: await KvStore.open({
			rocksdb,
			prefix: new Uint8Array([0]),
			key: Bytes32,
			value: StoredPointer,
		}),
		pubkey: await KvStore.open({
			rocksdb,
			prefix: new Uint8Array([1]),
			key: Bytes32,
			value: StoredPointer,
		}),
		spender: await IndexStore.open({
			path: join(BASE_DATA_DIR, "spender"),
			codec: StoredPointer,
			itemsPerChunk: 1_000_000,
		}),
	},
});

await atomic.recover();

export class ChainStore {
	public readonly blockHashToHeightMap: Uint8ArrayMap<StoredPointer>;
	public readonly atomic = atomic;

	private constructor(initialHeaders: WireBlockHeader[]) {
		this.blockHashToHeightMap = new Uint8ArrayMap<number>(Math.max(256, initialHeaders.length * 2));
		for (let i = 0; i < initialHeaders.length; i++) {
			this.blockHashToHeightMap.set(initialHeaders[i]!.hash(), i);
		}
	}

	static async open(p2pWorker: Worker) {
		const initialHeaderLength = atomic.stores.header.length();
		const intialHeaders = await atomic.stores.header.slice(0, initialHeaderLength);
		console.log(`[chain] loading headers count=${initialHeaderLength}`);
		console.log(`[chain] headers loaded`);

		const self = new ChainStore(headers);
		if (headers.length === 0) {
			await atomic.stores.header.truncate(0);
			await atomic.stores.block.truncate(0);
			await atomic.stores.spender.truncate(0);
			await atomic.stores.tx.truncate(0);
			await atomic.stores.txid.clear();
			await atomic.stores.pubkey.clear();
			const [genesisBlock] = WireBlock.decode(GENESIS_BLOCK);

			const batch = atomic.batch();

			self._pushHeaders([genesisBlock.header], batch);
			await self._appendTxs(genesisBlock.txs, 0, batch);

			batch.header.apply();
			batch.block.apply();
			batch.spender.apply();
			batch.tx.apply();
			batch.txid.apply();
			batch.pubkey.apply();
			await atomic.flush();
		}

		return self;
	}

	private _pushHeaders(headers: WireBlockHeader[], batches?: InferBatches<typeof atomic, "header">): { height: number } {
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

	private async _appendTxs(
		wireTxs: WireTx[],
		height: number,
		batches?: InferBatches<typeof atomic>,
	): Promise<{ pointer: StoredPointer }> {
		const batch = batches ?? atomic.batch();

		const op = async () => {
			const txs = wireTxs.map((wireTx) => StoredTx.fromWire(wireTx));
			const txCountBytes = StoredTxs.counter.encode(txs.length);
			const blockPointer = batch.tx.append(txCountBytes);

			// --- PHASE 1: prefetch all RocksDB reads up front, in parallel ---
			// We don't know same-block pointers yet, so we just prefetch *whether* each key
			// exists in RocksDB. Same-block cases are handled by local maps in phase 2.

			// input prevOut txids (skip raw-coinbase; same-block ones resolve locally in phase 2)
			const txidKeys = new Uint8ArrayMap<number>(64);
			// output scriptPubKey hashes
			const pubkeyKeys = new Uint8ArrayMap<number>(64);
			const outputHashes: (Uint8Array | null)[][] = []; // [t][i] -> hash or null, computed once
			for (let t = 0; t < txs.length; t++) {
				const tx = txs[t]!;
				for (const input of tx.inputs) {
					if (input.prevOut.txId.kind === "raw") txidKeys.set(input.prevOut.txId.value, 1);
				}
				const hashes: (Uint8Array | null)[] = [];
				for (const output of tx.outputs) {
					if (output.scriptPubKey.kind === "pointer") {
						hashes.push(null);
						continue;
					}
					const raw = rawScriptPubKey(output.scriptPubKey);
					hashes.push(sha256(raw)); // computed ONCE here, reused everywhere below
					pubkeyKeys.set(hashes[hashes.length - 1]!, 1);
				}
				outputHashes.push(hashes);
			}

			const txidPrefetch = new Uint8ArrayMap<StoredPointer>(64);
			const pubkeyPrefetch = new Uint8ArrayMap<StoredPointer>(64);
			await Promise.all([
				...[...txidKeys.keys()].map(async (id) => {
					const p = await batch.txid.get(id);
					if (p !== undefined) txidPrefetch.set(id, p);
				}),
				...[...pubkeyKeys.keys()].map(async (h) => {
					const p = await batch.pubkey.get(h);
					if (p !== undefined) pubkeyPrefetch.set(h, p);
				}),
			]);

			// --- PHASE 2: sequential, no RocksDB awaits ---
			const blockTxIds = new Uint8ArrayMap<number>(txs.length * 2); // same-block txid -> pointer
			const blockPubkeys = new Uint8ArrayMap<StoredPointer>(64); // same-block hash -> pointer

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
				const hashes = outputHashes[t]!;
				for (let i = 0; i < tx.outputs.length; i++) {
					const output = tx.outputs[i]!;
					if (output.scriptPubKey.kind === "pointer") continue;
					const hash = hashes[i]!;
					const existing = blockPubkeys.get(hash) ?? pubkeyPrefetch.get(hash);
					if (existing !== undefined) output.scriptPubKey = { kind: "pointer", value: existing };
				}

				const encoded = StoredTx.encodeWithOffsets(tx);

				// pubkey index writes: reuse the same hashes, dedup via local + prefetch
				for (let i = 0; i < tx.outputs.length; i++) {
					const output = tx.outputs[i]!;
					batch.spender.push(0);
					if (output.scriptPubKey.kind === "pointer") continue;
					const hash = hashes[i]!;
					if (blockPubkeys.get(hash) === undefined && pubkeyPrefetch.get(hash) === undefined) {
						const ptr = txPointer + encoded.offsets.outputs[i]!;
						batch.pubkey.set(hash, ptr);
						blockPubkeys.set(hash, ptr); // so a later same-block output reuses it
					}
				}

				// spender index: still awaits — these reads depend on same-block writes (see note)
				for (let i = 0; i < tx.inputs.length; i++) {
					const input = tx.inputs[i]!;
					if (input.prevOut.txId.kind !== "pointer") continue;
					const txSpenderOffset = await batch.tx.get(input.prevOut.txId.value + Bytes32.stride.size, U40);
					const spenderIndex = txSpenderOffset + input.prevOut.vout;
					const spender = await batch.spender.get(spenderIndex);
					if (spender && spender > 0) {
						const txid = await batch.tx.get(input.prevOut.txId.value, Bytes32);
						throw new Error(`Output ${formatHash(txid)}:${input.prevOut.vout} is already spent.`);
					}
					batch.spender.set(spenderIndex, txPointer);
				}

				batch.tx.append(encoded.bytes);
				offset += encoded.bytes.length;
			}

			const currentLength = batch.block.length();
			if (currentLength !== height) throw new Error(`Unexpected length=${height}, got ${currentLength}`);
			batch.block.push(blockPointer);
			return blockPointer;
		};

		if (batches) {
			const blockPointer = await op();
			return { pointer: blockPointer };
		}

		try {
			const blockPointer = await op();
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

	async getHeaderByHeight(height: number): Promise<StoredBlockHeader | undefined> {
		const header = await atomic.stores.header.get(height);
		if (!header) return undefined;
		return header;
	}

	async getHeaderByRange(
		from: number,
		to: number,
	): Promise<Array<{ height: number; header: WireBlockHeader }>> {
		const headers = await atomic.stores.header.slice(from, to + 1);
		return headers.map((header, i) => ({ height: from + i, header: header }));
	}

	async getHeaderByHash(hash: Uint8Array): Promise<StoredBlockHeader | undefined> {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return await this.getHeaderByHeight(height);
	}

	async getTxByPointer(pointer: StoredPointer): Promise<StoredTx> {
		const storedTx = await atomic.stores.tx.get(pointer, StoredTx, { readAheadSize: 400_000 });
		return storedTx;
	}

	async getTxById(txId: Uint8Array): Promise<StoredTx | undefined> {
		const pointer = await atomic.stores.txid.get(txId);
		if (pointer === undefined) return undefined;
		return await this.getTxByPointer(pointer);
	}

	async getTxsByBlockPointer(pointer: StoredPointer): Promise<StoredTx[] | undefined> {
		const storedTxs = await atomic.stores.tx.get(pointer, StoredTxs, { readAheadSize: MAX_BLOCK_WEIGHT });
		if (!storedTxs) return undefined;
		return await Promise.all(storedTxs);
	}

	async getTxsByBlockHeight(height: number): Promise<StoredTx[] | undefined> {
		const pointer = height === 0 ? 0 : await atomic.stores.block.get(height);
		if (pointer === undefined) return undefined;
		return await this.getTxsByBlockPointer(pointer);
	}

	async getTxsByBlockHash(hash: Uint8Array): Promise<StoredTx[] | undefined> {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return await this.getTxsByBlockHeight(height);
	}

	async getHeightByHash(hash: Uint8Array): Promise<number | undefined> {
		return this.blockHashToHeightMap.get(hash);
	}

	async getHashByHeight(height: number): Promise<Uint8Array | undefined> {
		const header = await this.getHeaderByHeight(height);
		if (!header) return undefined;
		return header.hash();
	}

	async getTxPointerById(txId: Uint8Array): Promise<StoredPointer | undefined> {
		return await atomic.stores.txid.get(txId);
	}

	async getBlockPointerByHeight(height: number): Promise<StoredPointer | undefined> {
		return await atomic.stores.block.get(height);
	}

	async getBlockPointerByHash(hash: Uint8Array): Promise<StoredPointer | undefined> {
		const height = this.blockHashToHeightMap.get(hash);
		if (height === undefined) return undefined;
		return await this.getBlockPointerByHeight(height);
	}

	async getChainTip(): Promise<{ height: number; header: StoredBlockHeader } | undefined> {
		const height = atomic.stores.header.length() - 1;
		if (height < 0) return undefined;
		const header = await this.getHeaderByHeight(height);
		if (!header) return undefined;
		return { height, header };
	}

	async getTxOutputByPointer(
		pointer: number,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): Promise<StoredTxOutput> {
		return await (batches ?? atomic.stores).tx.get(pointer, StoredTxOutput);
	}

	async getScriptPubKey(
		output: StoredTxOutput,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): Promise<ScriptPubKey> {
		if (output.scriptPubKey.kind === "pointer") {
			const resolved = await this.getTxOutputByPointer(output.scriptPubKey.value, batches);
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

	async getRawScriptPubKey(
		output: StoredTxOutput,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): Promise<Uint8Array> {
		return rawScriptPubKey(await this.getScriptPubKey(output, batches));
	}

	async getPrevOutTxId(input: StoredTxInput): Promise<Uint8Array> {
		const txId = input.prevOut.txId;
		const { kind, value } = txId;
		if (kind === "raw") {
			return value;
		}

		if (kind === "pointer") {
			return await this.getTxByPointer(value).then((tx) => tx.txId);
		}

		if (kind === "coinbase") {
			return COINBASE_TXID;
		}

		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}
}
