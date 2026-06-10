import { sha256 } from "@noble/hashes/sha2";
import { U32 } from "@nomadshiba/codec";
import { concat } from "@std/bytes";
import { join } from "@std/path";
import { BASE_DATA_DIR, BASE_DIR } from "~/config.ts";
import { MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { PeerChain } from "~/lib/chain/PeerChain.ts";
import { PeerChainNode } from "~/lib/chain/PeerChainNode.ts";
import { Tx } from "~/lib/chain/Tx.ts";
import { Bytes32 } from "~/lib/codec/primitives.ts";
import { StoredBlock } from "~/lib/codec/stored/StoredBlock.ts";
import { StoredPointer } from "~/lib/codec/stored/StoredPointer.ts";
import { encodeStoredTxWithOutputOffsets, StoredTx } from "~/lib/codec/stored/StoredTx.ts";
import { StoredTxOutput, TxOutput } from "~/lib/codec/stored/StoredTxOutput.ts";
import { StoredTxs } from "~/lib/codec/stored/StoredTxs.ts";
import { WireBlock } from "~/lib/codec/wire/WireBlock.ts";
import { WireBlockHeader } from "~/lib/codec/wire/WireBlockHeader.ts";
import { WireTx } from "~/lib/codec/wire/WireTx.ts";
import { ArrayStoreBatch, createArrayStore } from "~/lib/storage/ArrayStore.ts";
import { BlobStoreBatch, createBlobStore } from "~/lib/storage/BlobStore.ts";
import { createKVStore, KVStoreBatch } from "~/lib/storage/KVStore.ts";
import { atomic, recover, Store } from "~/lib/storage/Store.ts";
import { Uint8ArrayMap } from "~/lib/Uint8ArrayMap.ts";
import { verifyProofOfWork, workFromHeader } from "./lib/chain/utils/pow.ts";

export const GENESIS_BLOCK_HEIGHT = 0;
export const GENESIS_BLOCK = new Uint8Array(285);
GENESIS_BLOCK.set([
	// --- Block header (80 bytes) ---
	// version
	...[0x01, 0x00, 0x00, 0x00],
	// prev block hash (32 bytes of zero)
	...[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
	...[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
	...[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
	...[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
	// merkle root
	...[0x3b, 0xa3, 0xed, 0xfd, 0x7a, 0x7b, 0x12, 0xb2],
	...[0x7a, 0xc7, 0x2c, 0x3e, 0x67, 0x76, 0x8f, 0x61],
	...[0x7f, 0xc8, 0x1b, 0xc3, 0x88, 0x8a, 0x51, 0x32],
	...[0x3a, 0x9f, 0xb8, 0xaa, 0x4b, 0x1e, 0x5e, 0x4a],
	// time
	...[0x29, 0xab, 0x5f, 0x49],
	// bits
	...[0xff, 0xff, 0x00, 0x1d],
	// nonce
	...[0x1d, 0xac, 0x2b, 0x7c],

	// --- Transaction counter ---
	...[0x01],

	// --- Coinbase transaction ---
	// version
	...[0x01, 0x00, 0x00, 0x00],
	// input count
	...[0x01],
	// prev output hash
	...[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
	...[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
	...[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
	...[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
	// prev output index
	...[0xff, 0xff, 0xff, 0xff],
	// script length
	...[0x4d],
	// scriptSig
	...[0x04, 0xff, 0xff, 0x00, 0x1d, 0x01, 0x04, 0x45],
	...[0x54, 0x68, 0x65, 0x20, 0x54, 0x69, 0x6d, 0x65],
	...[0x73, 0x20, 0x30, 0x33, 0x2f, 0x4a, 0x61, 0x6e],
	...[0x2f, 0x32, 0x30, 0x30, 0x39, 0x20, 0x43, 0x68],
	...[0x61, 0x6e, 0x63, 0x65, 0x6c, 0x6c, 0x6f, 0x72],
	...[0x20, 0x6f, 0x6e, 0x20, 0x62, 0x72, 0x69, 0x6e],
	...[0x6b, 0x20, 0x6f, 0x66, 0x20, 0x73, 0x65, 0x63],
	...[0x6f, 0x6e, 0x64, 0x20, 0x62, 0x61, 0x69, 0x6c],
	...[0x6f, 0x75, 0x74, 0x20, 0x66, 0x6f, 0x72, 0x20],
	...[0x62, 0x61, 0x6e, 0x6b, 0x73],
	// sequence
	...[0xff, 0xff, 0xff, 0xff],

	// output count
	...[0x01],
	// value (50 BTC)
	...[0x00, 0xf2, 0x05, 0x2a, 0x01, 0x00, 0x00, 0x00],
	// pkScript length
	...[0x43],
	// pkScript
	...[0x41, 0x04, 0x67, 0x8a, 0xfd, 0xb0, 0xfe, 0x55],
	...[0x48, 0x27, 0x19, 0x67, 0xf1, 0xa6, 0x71, 0x30],
	...[0xb7, 0x10, 0x5c, 0xd6, 0xa8, 0x28, 0xe0, 0x39],
	...[0x09, 0xa6, 0x79, 0x62, 0xe0, 0xea, 0x1f, 0x61],
	...[0xde, 0xb6, 0x49, 0xf6, 0xbc, 0x3f, 0x4c, 0xef],
	...[0x38, 0xc4, 0xf3, 0x55, 0x04, 0xe5, 0x1e, 0xc1],
	...[0x12, 0xde, 0x5c, 0x38, 0x4d, 0xf7, 0xba, 0x0b],
	...[0x8d, 0x57, 0x8a, 0x4c, 0x70, 0x2b, 0x6b, 0xf1],
	...[0x1d, 0x5f],
	...[0xac],
	// locktime
	...[0x00, 0x00, 0x00, 0x00],
]);
export const GENESIS_BLOCK_HEADER = GENESIS_BLOCK.subarray(0, WireBlockHeader.stride.size);
export const GENESIS_BLOCK_PREV_HASH = GENESIS_BLOCK_HEADER.subarray(
	WireBlockHeader.inner.shape.version.stride.size,
	WireBlockHeader.inner.shape.version.stride.size + WireBlockHeader.inner.shape.prevHash.stride.size,
);
export const GENESIS_BLOCK_HASH = sha256(sha256(GENESIS_BLOCK_HEADER));

// TODO: Rename to headers
const blockStore = await createArrayStore({
	name: "blocks",
	path: join(BASE_DATA_DIR, "blocks"),
	codec: StoredBlock,
	counter: U32,
});

// TODO: Rename to txs
const blobStore = await createBlobStore({
	name: "txs",
	path: join(BASE_DATA_DIR, "txs"),
});

const txIdToPointer = await createKVStore({
	name: "txIdToPointer",
	path: join(BASE_DATA_DIR, "txIdToPointer"),
	keyCodec: Bytes32,
	valueCodec: StoredPointer,
	shards: 16,
});

const pubKeyToPointer = await createKVStore({
	name: "scriptPubKeyToPointer",
	path: join(BASE_DATA_DIR, "scriptPubKeyToPointer"),
	keyCodec: Bytes32,
	valueCodec: StoredPointer,
	shards: 16,
});

const stores: readonly Store[] = [
	blobStore,
	blockStore,
	txIdToPointer,
	pubKeyToPointer,
];

console.log("Stores initialized. Recovering data if needed…");
await recover(stores);
console.log("Recovery complete.");

function heapMB(): string {
	return `heap=${(Deno.memoryUsage().heapUsed / 1024 / 1024).toFixed(1)}MB`;
}

export const localChain = new PeerChain([]);

console.log(`[chain] loading blocks count=${blockStore.length()} ${heapMB()}`);
const blocks = await blockStore.slice(0, blockStore.length());
console.log(`[chain] blocks loaded ${heapMB()}`);

// In-memory hash → height index. Rebuilt from blockStore on every startup.
// No persistence needed — blockStore is the source of truth.
const hashToHeight = new Uint8ArrayMap<number>(Math.max(256, blocks.length * 2));
for (let i = 0; i < blocks.length; i++) {
	hashToHeight.set(blocks[i]!.header.hash, i);
}

localChain.clear();
if (blocks.length > 0) {
	let cumulativeWork = 0n;
	localChain.concat(blocks.map((block, height) => {
		if (!verifyProofOfWork(block.header)) {
			throw new Error();
		}
		cumulativeWork += workFromHeader(block.header);
		return new PeerChainNode({
			header: block.header,
			cumulativeWork,
			pointer: block.pointer ? block.pointer : (height ? null : 0),
		});
	}));
	console.log(`[chain] chain built height=${localChain.height()} ${heapMB()}`);
} else {
	await blockStore.truncate(0);
	await blobStore.truncate(0);
	await txIdToPointer.clear();
	await pubKeyToPointer.clear();
	hashToHeight.clear();
	const [genesisBlock] = WireBlock.decode(GENESIS_BLOCK);
	const cumulativeWork = workFromHeader(genesisBlock.header);
	const blockStoreBatch = blockStore.batch();
	const blobStoreBatch = blobStore.batch();
	const txIdToPointerBatch = txIdToPointer.batch();
	const pubKeyToPointerBatch = pubKeyToPointer.batch();

	appendBlockHeader([genesisBlock.header], { blockStoreBatch });
	const { pointer } = await appendBlockTxs(genesisBlock.txs, 0, {
		blobStoreBatch,
		blockStoreBatch,
		txIdToPointerBatch,
		pubKeyToPointerBatch,
	});
	localChain.push(new PeerChainNode({ header: genesisBlock.header, cumulativeWork, pointer }));

	blockStoreBatch.apply();
	blobStoreBatch.apply();
	txIdToPointerBatch.apply();
	pubKeyToPointerBatch.apply();
	await atomicSave();
}

export async function atomicSave() {
	try {
		await atomic(stores);
	} catch (reason) {
		console.error("Atomic save failed:", reason);
		Deno.exit(1);
	}
}

export function appendBlockHeader(
	headers: WireBlockHeader[],
	storeBatches?: {
		blockStoreBatch: ArrayStoreBatch<typeof StoredBlock>;
	},
): { height: number } {
	const { blockStoreBatch } = storeBatches ?? {
		blockStoreBatch: blockStore.batch(),
	};

	const op = () => {
		for (const header of headers) {
			const height = blockStoreBatch.push({ header, pointer: 0 });
			hashToHeight.set(header.hash, height);
		}
	};

	if (storeBatches) {
		op();
		return { height: blockStoreBatch.length() - 1 };
	}

	try {
		op();
		blockStoreBatch.apply();
		return { height: blockStoreBatch.length() - 1 };
	} catch (reason) {
		blockStoreBatch.discard();
		console.error("Failed to append block header:", reason);
		Deno.exit(1);
	}
}

export async function appendBlockTxs(
	wireTxs: WireTx[],
	height: number,
	storeBatches?: {
		blobStoreBatch: BlobStoreBatch;
		blockStoreBatch: ArrayStoreBatch<typeof StoredBlock>;
		txIdToPointerBatch: KVStoreBatch<Uint8Array, number>;
		pubKeyToPointerBatch: KVStoreBatch<Uint8Array, number>;
	},
): Promise<{ pointer: StoredPointer }> {
	const { blobStoreBatch, blockStoreBatch, txIdToPointerBatch, pubKeyToPointerBatch } = storeBatches ?? {
		blobStoreBatch: blobStore.batch(),
		blockStoreBatch: blockStore.batch(),
		txIdToPointerBatch: txIdToPointer.batch(),
		pubKeyToPointerBatch: pubKeyToPointer.batch(),
	};

	const op = async () => {
		const block = await blockStoreBatch.get(height);
		if (!block) {
			throw new Error(`Block at height ${height} not found`);
		}

		const txs = await Promise.all(wireTxs.map((wireTx) => Tx.fromWire(wireTx)));
		const txCountBytes = StoredTxs.counter.encode(txs.length);

		// blockPointer is where this block's blob will land — batch.size() gives the current end.
		const blockPointer = blobStoreBatch.size();

		// Process each tx: dedup scriptPubKeys first, then encode with correct final offsets.
		// We must process sequentially so each tx's final size is known before computing the
		// next tx's pointer offset.
		const encodedTxs: ReturnType<typeof encodeStoredTxWithOutputOffsets>[] = [];
		let offset = txCountBytes.length;
		for (let t = 0; t < txs.length; t++) {
			const tx = txs[t]!;
			const txPointer = blockPointer + offset;
			txIdToPointerBatch.set(tx.data.txId, txPointer);

			// Resolve any raw prevOuts using the batch (covers intra-block spends).
			for (let i = 0; i < tx.data.inputs.length; i++) {
				const input = tx.data.inputs[i]!;
				if (input.prevOut.txId.kind !== "raw") continue;
				const pointer = await txIdToPointerBatch.get(input.prevOut.txId.value);
				if (pointer === undefined) {
					console.error(
						`[appendBlockTxs] could not resolve prevOut to pointer at height=${height} tx=${t} vin=${i}: txId=${
							Array.from(input.prevOut.txId.value).map((b) => b.toString(16).padStart(2, "0")).join("")
						}`,
					);
					Deno.exit(1);
				}
				input.prevOut.txId = { kind: "pointer", value: pointer };
			}

			// Dedup scriptPubKey outputs by mutating tx.data.outputs before encoding.
			for (let i = 0; i < tx.data.outputs.length; i++) {
				const output = tx.data.outputs[i]!;
				if (output.scriptPubKey.kind === "pointer") continue;
				const raw = await TxOutput.getRawScriptPubKey(output);
				const hash = sha256(raw);
				const existing = await pubKeyToPointerBatch.get(hash);
				if (existing !== undefined) {
					output.scriptPubKey = { kind: "pointer", value: existing };
				}
				// Registration (pubKeyToPointerBatch.set) happens after encoding below,
				// once we know the correct final voutOffset for this output.
			}

			// Encode with deduped outputs to get correct final voutOffsets.
			const encoded = encodeStoredTxWithOutputOffsets(tx.toStore());
			encodedTxs.push(encoded);

			// Register new scriptPubKeys using final offsets.
			for (let i = 0; i < tx.data.outputs.length; i++) {
				const output = tx.data.outputs[i]!;
				if (output.scriptPubKey.kind === "pointer") continue;
				const raw = await TxOutput.getRawScriptPubKey(output);
				const hash = sha256(raw);
				if (await pubKeyToPointerBatch.get(hash) === undefined) {
					pubKeyToPointerBatch.set(hash, txPointer + encoded.voutOffsets[i]!);
				}
			}

			offset += encoded.bytes.length;
		}

		const fullBlob = concat([txCountBytes, ...encodedTxs.map((e) => e.bytes)]);
		const appendedPointer = blobStoreBatch.append(fullBlob);
		blockStoreBatch.set(height, { header: block.header, pointer: appendedPointer });

		return blockPointer;
	};

	if (storeBatches) {
		const blockPointer = await op();
		return { pointer: blockPointer };
	}

	try {
		const blockPointer = await op();
		blobStoreBatch.apply();
		blockStoreBatch.apply();
		txIdToPointerBatch.apply();
		pubKeyToPointerBatch.apply();

		return { pointer: blockPointer };
	} catch (reason) {
		blobStoreBatch.discard();
		blockStoreBatch.discard();
		txIdToPointerBatch.discard();
		pubKeyToPointerBatch.discard();
		console.error("Failed to append block body:", reason);
		Deno.exit(1);
	}
}

export async function getBlockByHeight(height: number): Promise<StoredBlock | undefined> {
	const block = await blockStore.get(height);
	if (!block) return undefined;
	return block;
}

export async function getBlocksByHeightRange(
	from: number,
	to: number,
): Promise<Array<{ height: number; header: WireBlockHeader }>> {
	const blocks = await blockStore.slice(from, to + 1);
	return blocks.map((block, i) => ({ height: from + i, header: block.header }));
}

export async function getBlockByHash(hash: Uint8Array): Promise<StoredBlock | undefined> {
	const height = hashToHeight.get(hash);
	if (height === undefined) return undefined;
	return await getBlockByHeight(height);
}

export async function getTxByPointer(pointer: StoredPointer): Promise<Tx> {
	const storedTx = await blobStore.get(pointer, StoredTx, { readAheadSize: 400_000 });
	return Tx.fromStore(storedTx);
}

export async function getTxById(txId: Uint8Array): Promise<Tx | undefined> {
	const pointer = await txIdToPointer.get(txId);
	if (pointer === undefined) return undefined;
	return await getTxByPointer(pointer);
}

export async function getTxsByBlockPointer(pointer: StoredPointer): Promise<Tx[] | undefined> {
	const storedTxs = await blobStore.get(pointer, StoredTxs, { readAheadSize: MAX_BLOCK_WEIGHT });
	if (!storedTxs) return undefined;
	return await Promise.all(storedTxs.map(Tx.fromStore));
}

export async function getTxsByBlockHeight(height: number): Promise<Tx[] | undefined> {
	const { pointer } = await blockStore.get(height);
	if (pointer === 0 && height !== 0) return undefined;
	return await getTxsByBlockPointer(pointer);
}

export async function getTxsByBlockHash(hash: Uint8Array): Promise<Tx[] | undefined> {
	const height = hashToHeight.get(hash);
	if (height === undefined) return undefined;
	return await getTxsByBlockHeight(height);
}

export async function getHeightByHash(hash: Uint8Array): Promise<number | undefined> {
	return hashToHeight.get(hash);
}

export async function getHashByHeight(height: number): Promise<Uint8Array | undefined> {
	const block = await getBlockByHeight(height);
	if (!block) return undefined;
	return block.header.hash;
}

export async function getTxPointerById(txId: Uint8Array): Promise<StoredPointer | undefined> {
	return await txIdToPointer.get(txId);
}

export async function getBlockPointerByHeight(height: number): Promise<StoredPointer | undefined> {
	return (await blockStore.get(height)).pointer;
}

export async function getBlockPointerByHash(hash: Uint8Array): Promise<StoredPointer | undefined> {
	const height = hashToHeight.get(hash);
	if (height === undefined) return undefined;
	return await getBlockPointerByHeight(height);
}

export async function getChainTip(): Promise<{ height: number; block: StoredBlock } | undefined> {
	const height = blockStore.length() - 1;
	if (height < 0) return undefined;
	const block = await getBlockByHeight(height);
	if (!block) return undefined;
	return { height, block };
}

export async function getTxOutputByPointer(pointer: number): Promise<TxOutput> {
	const output = await blobStore.get(pointer, StoredTxOutput);
	return output;
}

// ---------------------------------------------------------------------------
// Storage snapshot — appended to STORAGE.md after every atomicSave
// ---------------------------------------------------------------------------

async function dirSizeMiB(dir: string): Promise<number> {
	let bytes = 0;
	try {
		for await (const entry of Deno.readDir(dir)) {
			if (entry.isFile) {
				const stat = await Deno.stat(join(dir, entry.name));
				bytes += stat.size;
			} else if (entry.isDirectory) {
				bytes += await dirSizeMiB(join(dir, entry.name)) * 1024 * 1024;
			}
		}
	} catch {
		// dir may not exist yet
	}
	return bytes / (1024 * 1024);
}

const STORAGE_MD = join(BASE_DIR, "STORAGE.md");
const SATOSHI_JSON = join(BASE_DIR, "satoshi-client-blocks-size.json");

let _satoshiData: Array<{ x: number; y: number }> | null = null;
async function getSatoshiData(): Promise<Array<{ x: number; y: number }>> {
	if (_satoshiData) return _satoshiData;
	const json = JSON.parse(await Deno.readTextFile(SATOSHI_JSON));
	_satoshiData = json["blocks-size"];
	return _satoshiData!;
}

/** Return the largest entry whose timestamp is strictly before blockTimestampSec. */
async function satoshiMiBAtTimestamp(blockTimestampSec: number): Promise<number | null> {
	const data = await getSatoshiData();
	const blockMs = blockTimestampSec * 1000;
	let floor: { x: number; y: number } | null = null;
	for (const entry of data) {
		if (entry.x < blockMs) floor = entry;
		else break;
	}
	if (!floor) return null;
	return floor.y; // already in MiB
}

/** Measure disk usage and append a row to STORAGE.md. */
export async function appendStorageSnapshot(height: number, blockTimestampSec: number): Promise<void> {
	const txsMiB = await dirSizeMiB(join(BASE_DATA_DIR, "txs"));
	const totalMiB = await dirSizeMiB(BASE_DATA_DIR);
	const satoshiMiB = await satoshiMiBAtTimestamp(blockTimestampSec);

	const fmt = (v: number) => `~${Math.round(v).toLocaleString("en-US")}`;
	const savedMiB = satoshiMiB !== null ? satoshiMiB - txsMiB : null;
	const savedPct = satoshiMiB !== null && satoshiMiB > 0 ? (savedMiB! / satoshiMiB) * 100 : null;

	const col1 = satoshiMiB !== null ? fmt(satoshiMiB) : "-";
	const col2 = fmt(txsMiB);
	const col3 = savedMiB !== null ? fmt(savedMiB) : "-";
	const col4 = savedPct !== null ? `~${savedPct.toFixed(1)}%` : "-";
	const col5 = fmt(totalMiB);

	const row = `| ${height} | ${col1} | ${col2} | ${col3} | ${col4} | ${col5} |\n`;

	await Deno.writeTextFile(STORAGE_MD, row, { append: true });
	console.log(
		`[storage] snapshot height=${height} txs=${fmt(txsMiB)}MiB total=${fmt(totalMiB)}MiB satoshi=${col1}MiB`,
	);
}
