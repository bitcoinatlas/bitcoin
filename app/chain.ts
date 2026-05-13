import { sha256 } from "@noble/hashes/sha2";
import { U32, U32LE } from "@nomadshiba/codec";
import { join } from "@std/path";
import { BASE_DATA_DIR } from "~/config.ts";
import { PeerChain } from "~/lib/chain/PeerChain.ts";
import { PeerChainNode } from "~/lib/chain/PeerChainNode.ts";
import { Tx } from "~/lib/chain/Tx.ts";
import { Bytes32 } from "~/lib/codec/primitives.ts";
import { StoredBlock } from "~/lib/codec/stored/StoredBlock.ts";
import { StoredPointer } from "~/lib/codec/stored/StoredPointer.ts";
import { StoredTx } from "~/lib/codec/stored/StoredTx.ts";
import { StoredTxs } from "~/lib/codec/stored/StoredTxs.ts";
import { WireBlock } from "~/lib/codec/wire/WireBlock.ts";
import { WireBlockHeader } from "~/lib/codec/wire/WireBlockHeader.ts";
import { WireTx } from "~/lib/codec/wire/WireTx.ts";
import { ArrayStoreTransaction, createArrayStore } from "~/lib/storage/ArrayStore.ts";
import { BlobStoreTransaction, createBlobStore } from "~/lib/storage/BlobStore.ts";
import { createKVStore, KVStoreTransaction } from "~/lib/storage/KVStore.ts";
import { atomic, recover, Store } from "~/lib/storage/Store.ts";
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
export const GENESIS_BLOCK_HEADER = GENESIS_BLOCK.subarray(0, WireBlockHeader.stride);
export const GENESIS_BLOCK_PREV_HASH = GENESIS_BLOCK_HEADER.subarray(
	WireBlockHeader.inner.shape.version.stride,
	WireBlockHeader.inner.shape.version.stride + WireBlockHeader.inner.shape.prevHash.stride,
);
export const GENESIS_BLOCK_HASH = sha256(sha256(GENESIS_BLOCK_HEADER));

export const KNOWN_ADDRESS_TYPES = [
	"p2pkh",
	"p2sh",
	"p2wpkh",
	"p2wsh",
	"p2tr",
] as const;

const blockStore = await createArrayStore({
	name: "blocks",
	path: join(BASE_DATA_DIR, "blocks"),
	codec: StoredBlock,
	countCodec: U32,
});

const blobStore = await createBlobStore({
	name: "txs",
	path: join(BASE_DATA_DIR, "txs"),
});

const blockHashToHeight = await createKVStore({
	name: "hashToHeight",
	path: join(BASE_DATA_DIR, "hashToHeight"),
	keyCodec: Bytes32,
	valueCodec: U32LE,
});

const txIdToPointer = await createKVStore({
	name: "txIdToPointer",
	path: join(BASE_DATA_DIR, "txIdToPointer"),
	keyCodec: Bytes32,
	valueCodec: StoredPointer,
});

const stores: readonly Store[] = [
	blobStore,
	blockStore,
	txIdToPointer,
	blockHashToHeight,
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
	await blockHashToHeight.clear();
	const [genesisBlock] = WireBlock.decode(GENESIS_BLOCK);
	const cumulativeWork = workFromHeader(genesisBlock.header);
	const blockStoreTx = blockStore.transaction();
	const blobStoreTx = blobStore.transaction();
	const txIdToPointerTx = txIdToPointer.transaction();
	const blockHashToHeightTx = blockHashToHeight.transaction();

	try {
		appendBlockHeader([genesisBlock.header], { blockStoreTx, blockHashToHeightTx });
		const { pointer } = await appendBlockTxs(genesisBlock.txs, 0, { blobStoreTx, blockStoreTx, txIdToPointerTx });
		localChain.push(new PeerChainNode({ header: genesisBlock.header, cumulativeWork, pointer }));

		blockStoreTx.apply();
		blobStoreTx.apply();
		txIdToPointerTx.apply();
		blockHashToHeightTx.apply();
		await atomicSave();
	} catch (reason) {
		console.error("Pushing genesis block failed:", reason);
		Deno.exit(1);
	}
}

export async function atomicSave() {
	console.log(`[chain] atomicSave start blobStaged=${blobStore.length()} ${heapMB()}`);
	await atomic(stores);
	console.log(`[chain] atomicSave done ${heapMB()}`);
}

export function appendBlockHeader(
	headers: WireBlockHeader[],
	storeTxs?: {
		blockStoreTx: ArrayStoreTransaction<StoredBlock>;
		blockHashToHeightTx: KVStoreTransaction<Uint8Array, number>;
	},
): { height: number } {
	const { blockStoreTx, blockHashToHeightTx } = storeTxs ?? {
		blockStoreTx: blockStore.transaction(),
		blockHashToHeightTx: blockHashToHeight.transaction(),
	};

	const op = () => {
		for (const header of headers) {
			const height = blockStoreTx.append({ header, pointer: 0 });
			blockHashToHeightTx.set(header.hash, height);
		}
	};

	if (storeTxs) {
		op();
		return { height: blockStoreTx.length() - 1 };
	}

	try {
		op();
		blockStoreTx.apply();
		blockHashToHeightTx.apply();
		return { height: blockStoreTx.length() - 1 };
	} catch (reason) {
		blockStoreTx.discard();
		blockHashToHeightTx.discard();
		console.error("Failed to append block header:", reason);
		Deno.exit(1);
	}
}

export async function appendBlockTxs(
	wireTxs: WireTx[],
	height: number,
	storeTxs?: {
		blobStoreTx: BlobStoreTransaction;
		blockStoreTx: ArrayStoreTransaction<StoredBlock>;
		txIdToPointerTx: KVStoreTransaction<Uint8Array, number>;
	},
): Promise<{ pointer: StoredPointer }> {
	const { blobStoreTx, blockStoreTx, txIdToPointerTx } = storeTxs ?? {
		blobStoreTx: blobStore.transaction(),
		blockStoreTx: blockStore.transaction(),
		txIdToPointerTx: txIdToPointer.transaction(),
	};

	const op = async () => {
		const block = await blockStoreTx.get(height);
		if (!block) {
			throw new Error(`Block at height ${height} not found`);
		}

		const txs = await Promise.all(wireTxs.map((wireTx) => Tx.fromWire(wireTx)));
		const txCountBytes = StoredTxs.countCodec.encode(txs.length);
		const blockPointer = blobStoreTx.append(txCountBytes);
		blockStoreTx.set(height, { header: block.header, pointer: blockPointer });

		for (const tx of txs) {
			const storedTx = tx.toStore();
			const storedTxBytes = StoredTx.encode(storedTx);
			const txPointer = blobStoreTx.append(storedTxBytes);
			txIdToPointerTx.set(tx.data.txId, txPointer);
		}

		return blockPointer;
	};

	if (storeTxs) {
		const blockPointer = await op();
		return { pointer: blockPointer };
	}

	try {
		const blockPointer = await op();
		blobStoreTx.apply();
		blockStoreTx.apply();
		txIdToPointerTx.apply();

		console.log(
			`[chain] appendBlockTxs height=${height} txs=${wireTxs.length} blobTotal=${blobStore.length()} ${heapMB()}`,
		);
		return { pointer: blockPointer };
	} catch (reason) {
		blobStoreTx.discard();
		blockStoreTx.discard();
		txIdToPointerTx.discard();
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
	const height = await blockHashToHeight.get(hash);
	if (height === undefined) return undefined;
	return await getBlockByHeight(height);
}

export async function getTxByPointer(pointer: StoredPointer): Promise<Tx> {
	const storedTx = await blobStore.get(pointer, StoredTx);
	return Tx.fromStore(storedTx);
}

export async function getTxById(txId: Uint8Array): Promise<Tx | undefined> {
	const pointer = await txIdToPointer.get(txId);
	if (pointer === undefined) return undefined;
	return await getTxByPointer(pointer);
}

export async function getTxsByBlockPointer(pointer: StoredPointer): Promise<Tx[] | undefined> {
	const storedTxs = await blobStore.get(pointer, StoredTxs);
	if (!storedTxs) return undefined;
	return await Promise.all(storedTxs.map(Tx.fromStore));
}

export async function getTxsByBlockHeight(height: number): Promise<Tx[] | undefined> {
	const { pointer } = await blockStore.get(height);
	if (pointer === undefined) return undefined;
	return await getTxsByBlockPointer(pointer);
}

export async function getTxsByBlockHash(hash: Uint8Array): Promise<Tx[] | undefined> {
	const height = await blockHashToHeight.get(hash);
	if (height === undefined) return undefined;
	return await getTxsByBlockHeight(height);
}

export async function getHeightByHash(hash: Uint8Array): Promise<number | undefined> {
	return await blockHashToHeight.get(hash);
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
	const height = await blockHashToHeight.get(hash);
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
