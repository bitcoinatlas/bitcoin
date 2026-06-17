import { sha256 } from "@noble/hashes/sha2";
import { join } from "@std/path";
import { formatHash } from "~/api/frontend/utils/format.ts";
import { ns } from "~/chain/ns.ts";
import { PeerChain } from "~/chain/PeerChain.ts";
import { GENESIS_BLOCK } from "~/chain/utils/genesis.ts";
import { verifyProofOfWork, workFromHeader } from "~/chain/utils/pow.ts";
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
import { BASE_DATA_DIR } from "~/config.ts";
import { MAX_BLOCK_SIZE, MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { ArrayStore } from "~/storage/ArrayStore.ts";
import { Atomic, InferBatches, InferStores } from "~/storage/Atomic.ts";
import { BlobStore } from "~/storage/BlobStore.ts";
import { IndexStore } from "~/storage/IndexStore.ts";
import { Uint8ArrayMap } from "~/utils/Uint8ArrayMap.ts";
import { KvStore } from "~/storage/KvStore.ts";
import { RocksDatabase } from "@harperfast/rocksdb-js";

RocksDatabase.config({
	blockCacheSize: 4 * 1024 * 1024 * 1024,
	writeBufferManagerSize: 512 * 1024 * 1024,
	writeBufferManagerCostToCache: true,
});

const rocksDir = join(BASE_DATA_DIR, "rocksdb");
await Deno.mkdir(rocksDir, { recursive: true });
const rocksdb = RocksDatabase.open(join(BASE_DATA_DIR, "rocksdb"), {
	disableWAL: true,
	parallelismThreads: Math.min(6, navigator.hardwareConcurrency),
});

export const atomic = await Atomic.open({
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

console.log("Stores initialized. Recovering data if needed…");
await atomic.recover();
console.log("Recovery complete.");

function heapMiB(): string {
	return `heap=${(Deno.memoryUsage().heapUsed / 1024 / 1024).toFixed(1)}MiB`;
}

export const localChain = new PeerChain([]);

const initialHeaderLength = atomic.stores.header.length();
const initialBlockLength = atomic.stores.block.length();
console.log(`[chain] loading headers count=${initialHeaderLength}`);
const headers = await atomic.stores.header.slice(0, initialHeaderLength);
const blocks = await atomic.stores.block.slice(0, initialBlockLength);
console.log(`[chain] headers loaded`);

const headerHashMap = new Uint8ArrayMap<number>(Math.max(256, headers.length * 2));
for (let i = 0; i < headers.length; i++) {
	headerHashMap.set(headers[i]!.hash(), i);
}

localChain.clear();
if (headers.length > 0) {
	let cumulativeWork = 0n;
	localChain.concat(headers.map((header, height) => {
		const pointer = blocks[height];
		if (!verifyProofOfWork(header)) {
			throw new Error();
		}
		cumulativeWork += workFromHeader(header);
		return {
			header,
			cumulativeWork,
			pointer: pointer ? pointer : (height ? null : 0),
		};
	}));
	console.log(`[chain] chain built height=${localChain.height()} ${heapMiB()}`);
} else {
	await atomic.stores.header.truncate(0);
	await atomic.stores.block.truncate(0);
	await atomic.stores.spender.truncate(0);
	await atomic.stores.tx.truncate(0);
	await atomic.stores.txid.clear();
	await atomic.stores.pubkey.clear();
	headerHashMap.clear();
	const [genesisBlock] = WireBlock.decode(GENESIS_BLOCK);
	const cumulativeWork = workFromHeader(genesisBlock.header);

	const batch = atomic.batch();

	appendHeader([genesisBlock.header], batch);
	const { pointer } = await appendTxs(genesisBlock.txs, 0, batch);
	localChain.push({ header: genesisBlock.header, cumulativeWork, pointer });

	batch.header.apply();
	batch.block.apply();
	batch.spender.apply();
	batch.tx.apply();
	batch.txid.apply();
	batch.pubkey.apply();
	await atomic.flush();
}

export function appendHeader(
	headers: WireBlockHeader[],
	batches?: InferBatches<typeof atomic, "header">,
): { height: number } {
	const batch = batches ?? atomic.batch(["header"]);

	const op = () => {
		for (const header of headers) {
			const height = batch.header.push(header);
			headerHashMap.set(header.hash(), height);
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

export async function appendTxs(
	wireTxs: WireTx[],
	height: number,
	batches?: InferBatches<typeof atomic>,
): Promise<{ pointer: StoredPointer }> {
	const batch = batches ?? atomic.batch();

	const op = async () => {
		const txs = await Promise.all(wireTxs.map((wireTx) => ns.fromWire(wireTx)));
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
				const raw = await ns.getRawScriptPubKey(output, batch);
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
				if (spender > 0) {
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

export async function getHeaderByHeight(height: number): Promise<StoredBlockHeader | undefined> {
	const header = await atomic.stores.header.get(height);
	if (!header) return undefined;
	return header;
}

export async function getHeaderByRange(
	from: number,
	to: number,
): Promise<Array<{ height: number; header: WireBlockHeader }>> {
	const headers = await atomic.stores.header.slice(from, to + 1);
	return headers.map((header, i) => ({ height: from + i, header: header }));
}

export async function getHeaderByHash(hash: Uint8Array): Promise<StoredBlockHeader | undefined> {
	const height = headerHashMap.get(hash);
	if (height === undefined) return undefined;
	return await getHeaderByHeight(height);
}

export async function getTxByPointer(pointer: StoredPointer): Promise<StoredTx> {
	const storedTx = await atomic.stores.tx.get(pointer, StoredTx, { readAheadSize: 400_000 });
	return storedTx;
}

export async function getTxById(txId: Uint8Array): Promise<StoredTx | undefined> {
	const pointer = await atomic.stores.txid.get(txId);
	if (pointer === undefined) return undefined;
	return await getTxByPointer(pointer);
}

export async function getTxsByBlockPointer(pointer: StoredPointer): Promise<StoredTx[] | undefined> {
	const storedTxs = await atomic.stores.tx.get(pointer, StoredTxs, { readAheadSize: MAX_BLOCK_WEIGHT });
	if (!storedTxs) return undefined;
	return await Promise.all(storedTxs);
}

export async function getTxsByBlockHeight(height: number): Promise<StoredTx[] | undefined> {
	const pointer = await atomic.stores.block.get(height);
	if (pointer === 0 && height !== 0) return undefined;
	return await getTxsByBlockPointer(pointer);
}

export async function getTxsByBlockHash(hash: Uint8Array): Promise<StoredTx[] | undefined> {
	const height = headerHashMap.get(hash);
	if (height === undefined) return undefined;
	return await getTxsByBlockHeight(height);
}

export async function getHeightByHash(hash: Uint8Array): Promise<number | undefined> {
	return headerHashMap.get(hash);
}

export async function getHashByHeight(height: number): Promise<Uint8Array | undefined> {
	const header = await getHeaderByHeight(height);
	if (!header) return undefined;
	return header.hash();
}

export async function getTxPointerById(txId: Uint8Array): Promise<StoredPointer | undefined> {
	return await atomic.stores.txid.get(txId);
}

export async function getBlockPointerByHeight(height: number): Promise<StoredPointer | undefined> {
	return await atomic.stores.block.get(height);
}

export async function getBlockPointerByHash(hash: Uint8Array): Promise<StoredPointer | undefined> {
	const height = headerHashMap.get(hash);
	if (height === undefined) return undefined;
	return await getBlockPointerByHeight(height);
}

export async function getChainTip(): Promise<{ height: number; header: StoredBlockHeader } | undefined> {
	const height = atomic.stores.header.length() - 1;
	if (height < 0) return undefined;
	const header = await getHeaderByHeight(height);
	if (!header) return undefined;
	return { height, header };
}

export async function getTxOutputByPointer(
	pointer: number,
	batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
): Promise<StoredTxOutput> {
	return await (batches ?? atomic.stores).tx.get(pointer, StoredTxOutput);
}
