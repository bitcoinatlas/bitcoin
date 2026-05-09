import { U32LE } from "@nomadshiba/codec";
import { join } from "@std/path";
import { BASE_DATA_DIR, GENESIS_BLOCK_HEADER } from "~/constants.ts";
import { StoredBlock } from "~/lib/chain/codec/stored/StoredBlock.ts";
import { StoredPointer } from "~/lib/chain/codec/stored/StoredPointer.ts";
import { WireBlockHeader } from "~/lib/chain/codec/wire/WireBlockHeader.ts";
import { PeerChain } from "~/lib/chain/PeerChain.ts";
import { PeerChainNode } from "~/lib/chain/PeerChainNode.ts";
import { verifyProofOfWork, workFromHeader } from "~/lib/chain/utils/PoW.ts";
import { Bytes32, U48LE } from "~/lib/codec/primitives.ts";
import { createArrayStore } from "~/lib/storage/ArrayStore.ts";
import { createBlobStore } from "~/lib/storage/BlobStore.ts";
import { createKVStore } from "~/lib/storage/KVStore.ts";
import { atomic, recover, Store } from "~/lib/storage/Store.ts";
import { StoredTx } from "./codec/stored/StoredTx.ts";
import { Tx } from "./Tx.ts";
import { WireTx } from "./codec/wire/WireTx.ts";

const blockHashToHeight = await createKVStore<Uint8Array, number>({
	name: "hashToHeight",
	path: join(BASE_DATA_DIR, "hashToHeight"),
	keyCodec: Bytes32,
	valueCodec: U32LE,
});

const blockHeightToHeader = await createArrayStore<WireBlockHeader>({
	name: "headers",
	path: join(BASE_DATA_DIR, "headers"),
	codec: WireBlockHeader,
	countCodec: U48LE,
});

const blockHeightToPointer = await createArrayStore<StoredPointer>({
	name: "blockPointers",
	path: join(BASE_DATA_DIR, "blockPointers"),
	codec: StoredPointer,
	countCodec: U48LE,
});

const blocks = await createBlobStore({
	name: "blocks",
	path: join(BASE_DATA_DIR, "blocks"),
});

const unorderedBlocks = await createKVStore<Uint8Array, StoredBlock>({
	name: "unorderedBlocks",
	path: join(BASE_DATA_DIR, "unorderedBlocks"),
	keyCodec: Bytes32,
	valueCodec: StoredBlock,
});

const txIdToPointer = await createKVStore<Uint8Array, StoredPointer>({
	name: "txIdToPointer",
	path: join(BASE_DATA_DIR, "txIdToPointer"),
	keyCodec: Bytes32,
	valueCodec: StoredPointer,
});

const stores: readonly Store[] = [
	blockHashToHeight,
	blockHeightToHeader,
	blockHeightToPointer,
	blocks,
	unorderedBlocks,
	txIdToPointer,
];

await recover(stores);

export async function atomicFlush() {
	await atomic(stores);
}

const localChain = new PeerChain([]);

const headerLength = blockHeightToHeader.length();
const headers = await blockHeightToHeader.slice(0, headerLength);
localChain.clear();
if (headers.length > 0) {
	const pointers = await blockHeightToPointer.slice(0, blockHeightToPointer.length());
	let cumulativeWork = 0n;
	localChain.concat(headers.map((header, height) => {
		if (!verifyProofOfWork(header)) {
			throw new Error();
		}
		const pointer = pointers[height] ?? null;
		cumulativeWork += workFromHeader(header);
		return new PeerChainNode({ header, cumulativeWork, pointer });
	}));
} else {
	await blockHeightToPointer.truncate(0);
	await blockHeightToHeader.truncate(0);
	await blocks.truncate(0);
	await unorderedBlocks.clear();
	await txIdToPointer.clear();
	await blockHashToHeight.clear();
	const [header] = WireBlockHeader.decode(GENESIS_BLOCK_HEADER);
	const pointer = null;
	const cumulativeWork = workFromHeader(header);
	localChain.push(new PeerChainNode({ header, cumulativeWork, pointer }));
	const tx = blockHeightToHeader.transaction();
	tx.append(header);
	tx.apply();
	await atomic([blockHeightToHeader]);
}

export function appendBlockHeader(headers: WireBlockHeader[]): void {
	const heightToHeaderTx = blockHeightToHeader.transaction();
	const hashToHeightTx = blockHashToHeight.transaction();

	try {
		for (const header of headers) {
			const height = heightToHeaderTx.append(header);
			hashToHeightTx.set(header.hash, height);
		}
		heightToHeaderTx.apply();
		hashToHeightTx.apply();
	} catch (reason) {
		heightToHeaderTx.discard();
		hashToHeightTx.discard();
		console.error("Failed to append block header:", reason);
		Deno.exit(1);
	}
}

export function appendBlockBody(wireTxs: WireTx[]) {
	const blocksTx = blocks.transaction();
	const heightToPointerTx = blockHeightToPointer.transaction();
	const txIdToPointerTx = txIdToPointer.transaction();

	try {
		const txs = wireTxs.map((wireTx) => Tx.fromWire(wireTx));
		const storedBlock = StoredBlock.encode({ transactions: txs.map((tx) => tx.toStore()) });
		const pointer = blocksTx.append(storedBlock);
		const height = heightToPointerTx.append(pointer);
		for (const tx of txs) {
			txIdToPointerTx.set(tx.data.txId /* uhh we need to append txs one by one? */);
		}

		blocksTx.apply();
		heightToPointerTx.apply();
		txIdToPointerTx.apply();
	} catch (reason) {
		blocksTx.discard();
		heightToPointerTx.discard();
		txIdToPointerTx.discard();
		console.error("Failed to append block body:", reason);
		Deno.exit(1);
	}
}

export async function getBlockHeaderByHeight(height: number): Promise<WireBlockHeader | undefined> {
	const header = await blockHeightToHeader.get(height);
	if (!header) return undefined;
	return header;
}

export async function getBlockHeaderByHash(hash: Uint8Array): Promise<WireBlockHeader | undefined> {
	const height = await blockHashToHeight.get(hash);
	if (height === undefined) return undefined;
	return await getBlockHeaderByHeight(height);
}

export async function getTxByPointer(pointer: StoredPointer): Promise<Tx | undefined> {
	const storedTx = await blocks.get(pointer, StoredTx);
	if (!storedTx) return undefined;
	return Tx.fromStore(storedTx);
}

export async function getTxById(txId: Uint8Array): Promise<Tx | undefined> {
	const pointer = await txIdToPointer.get(txId);
	if (pointer === undefined) return undefined;
	return await getTxByPointer(pointer);
}

export async function getBlockBodyByPointer(pointer: StoredPointer): Promise<Tx[] | undefined> {
	const storedBlock = await blocks.get(pointer, StoredBlock);
	if (!storedBlock) return undefined;
	return await Promise.all(storedBlock.transactions.map(Tx.fromStore));
}

export async function getBlockBodyByHeight(height: number): Promise<Tx[] | undefined> {
	const pointer = await blockHeightToPointer.get(height);
	if (pointer === undefined) return undefined;
	return await getBlockBodyByPointer(pointer);
}

export async function getBlockBodyByHash(hash: Uint8Array): Promise<Tx[] | undefined> {
	const height = await blockHashToHeight.get(hash);
	if (height === undefined) return undefined;
	return await getBlockBodyByHeight(height);
}
