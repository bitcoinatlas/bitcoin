import { sha256 } from "@noble/hashes/sha2";
import { StructCodec } from "@nomadshiba/codec";
import { concat } from "@std/bytes";
import { join } from "@std/path";
import { PeerChain } from "~/chain/PeerChain.ts";
import { PeerChainNode } from "~/chain/PeerChainNode.ts";
import { Tx } from "~/chain/Tx.ts";
import { GENESIS_BLOCK } from "~/chain/utils/genesis.ts";
import { verifyProofOfWork, workFromHeader } from "~/chain/utils/pow.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxOutput, TxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { WireBlock } from "~/codec/wire/WireBlock.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { BASE_DATA_DIR } from "~/config.ts";
import { MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { ArrayStore } from "~/storage/ArrayStore.ts";
import { Atomic, InferBatches } from "~/storage/Atomic.ts";
import { BlobStore } from "~/storage/BlobStore.ts";
import { KVStore } from "~/storage/KVStore.ts";
import { Uint8ArrayMap } from "~/utils/Uint8ArrayMap.ts";
import { IndexStore } from "~/storage/IndexStore.ts";
import { formatHash } from "~/api/frontend/utils/format.ts";

export const atomic = await Atomic.open({
	path: join(BASE_DATA_DIR, "atomic"),
	stores: {
		header: await ArrayStore.open({
			path: join(BASE_DATA_DIR, "header"),
			codec: StoredBlockHeader,
		}),
		block: await ArrayStore.open({
			path: join(BASE_DATA_DIR, "block"),
			codec: StoredPointer,
		}),
		tx: await BlobStore.open({
			path: join(BASE_DATA_DIR, "tx"),
		}),
		txid: await KVStore.open({
			path: join(BASE_DATA_DIR, "txid"),
			keyCodec: Bytes32,
			valueCodec: new StructCodec({
				tx: StoredPointer,
				spentby: U40,
			}),
			shards: 16,
		}),
		pubkey: await KVStore.open({
			path: join(BASE_DATA_DIR, "pubkey"),
			keyCodec: Bytes32,
			valueCodec: StoredPointer,
			shards: 16,
		}),
		spentby: await IndexStore.open({
			path: join(BASE_DATA_DIR, "spentby"),
			codec: StoredPointer,
		}),
	},
});

console.log("Stores initialized. Recovering data if needed…");
await atomic.recover();
console.log("Recovery complete.");

function heapMB(): string {
	return `heap=${(Deno.memoryUsage().heapUsed / 1024 / 1024).toFixed(1)}MB`;
}

export const localChain = new PeerChain([]);

const initialHeaderLength = atomic.stores.header.length();
const initialBlockLength = atomic.stores.block.length();
console.log(`[chain] loading blocks count=${initialHeaderLength}`);
const headers = await atomic.stores.header.slice(0, initialHeaderLength);
const blocks = await atomic.stores.block.slice(0, initialBlockLength);
console.log(`[chain] blocks loaded`);

const headerHashMap = new Uint8ArrayMap<number>(Math.max(256, headers.length * 2));
for (let i = 0; i < headers.length; i++) {
	headerHashMap.set(headers[i]!.hash, i);
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
		return new PeerChainNode({
			header,
			cumulativeWork,
			pointer: pointer ? pointer : (height ? null : 0),
		});
	}));
	console.log(`[chain] chain built height=${localChain.height()} ${heapMB()}`);
} else {
	await atomic.stores.header.truncate(0);
	await atomic.stores.block.truncate(0);
	await atomic.stores.spentby.truncate(0);
	await atomic.stores.tx.truncate(0);
	await atomic.stores.txid.clear();
	await atomic.stores.pubkey.clear();
	headerHashMap.clear();
	const [genesisBlock] = WireBlock.decode(GENESIS_BLOCK);
	const cumulativeWork = workFromHeader(genesisBlock.header);

	const batch = atomic.batch();

	appendHeader([genesisBlock.header], batch);
	const { pointer } = await appendTxs(genesisBlock.txs, 0, batch);
	localChain.push(new PeerChainNode({ header: genesisBlock.header, cumulativeWork, pointer }));

	batch.header.apply();
	batch.block.apply();
	batch.spentby.apply();
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
			headerHashMap.set(header.hash, height);
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
		const txs = await Promise.all(wireTxs.map((wireTx) => Tx.fromWire(wireTx)));
		const txCountBytes = StoredTxs.counter.encode(txs.length);

		// blockPointer is where this block's blob will land — batch.size() gives the current end.
		const blockPointer = batch.tx.size();

		// Process each tx: dedup scriptPubKeys first, then encode with correct final offsets.
		// We must process sequentially so each tx's final size is known before computing the
		// next tx's pointer offset.
		const encodedTxs: ReturnType<typeof StoredTx.encodeWithOffsets>[] = [];
		let offset = txCountBytes.length;
		for (let t = 0; t < txs.length; t++) {
			const tx = txs[t]!;
			const txPointer = blockPointer + offset;
			batch.txid.set(tx.data.txId, { tx: txPointer, spentby: 0 });

			// Set prevOut pointers
			for (let i = 0; i < tx.data.inputs.length; i++) {
				const input = tx.data.inputs[i]!;
				if (input.prevOut.txId.kind !== "raw") continue;
				const pointer = await batch.txid.get(input.prevOut.txId.value);
				if (pointer === undefined) {
					console.error(
						`[appendTxs] could not resolve prevOut to pointer at height=${height} tx=${t} vin=${i}: txId=${
							Array.from(input.prevOut.txId.value).map((b) => b.toString(16).padStart(2, "0")).join("")
						}`,
					);
					Deno.exit(1);
				}

				input.prevOut.txId = { kind: "pointer", value: pointer.tx };
			}

			// Set scriptPubKey pointers
			for (let i = 0; i < tx.data.outputs.length; i++) {
				const output = tx.data.outputs[i]!;
				if (output.scriptPubKey.kind === "pointer") continue;
				const raw = await TxOutput.getRawScriptPubKey(output);
				const hash = sha256(raw);
				const existing = await batch.pubkey.get(hash);
				if (existing !== undefined) {
					output.scriptPubKey = { kind: "pointer", value: existing };
				}
			}

			// Encode the final tx
			const encoded = StoredTx.encodeWithOffsets(tx.toStore());
			encodedTxs.push(encoded);

			// Update pubkey index
			for (let i = 0; i < tx.data.outputs.length; i++) {
				const output = tx.data.outputs[i]!;
				if (output.scriptPubKey.kind === "pointer") continue;
				const raw = await TxOutput.getRawScriptPubKey(output);
				const hash = sha256(raw);
				if (await batch.pubkey.get(hash) === undefined) {
					batch.pubkey.set(hash, txPointer + encoded.offsets.vout[i]!);
				}
			}

			// Update spentby index
			for (let i = 0; i < tx.data.inputs.length; i++) {
				const input = tx.data.inputs[i]!;
				if (input.prevOut.txId.kind !== "pointer") continue; // can't be raw, and coinbase has no prevOut

				const txid = await getTxIdByPointer(input.prevOut.txId.value);
				const pointer = await batch.txid.get(txid);
				if (!pointer) {
					throw new Error("prevOut can't be found in txid index, corrupted data.");
				}

				const spender = await batch.spentby.get(pointer.spentby);
				if (spender > 0) {
					// TODO: This shouldn't throw normally, it should blacklist the block hash and skip to the next tick
					throw new Error(`Output ${formatHash(txid)}:${input.prevOut.vout} is already spent.`);
				}

				batch.spentby.set(pointer.spentby, encoded.offsets.vin[i]!);
			}

			offset += encoded.bytes.length;
		}

		const fullBlob = concat([txCountBytes, ...encodedTxs.map((e) => e.bytes)]);
		const appendedPointer = batch.tx.append(fullBlob);

		const currentLength = batch.block.length();
		if (currentLength !== height) {
			throw new Error(`Unexpected currentLength=${height}, got currentLength=${currentLength}`);
		}
		batch.block.push(appendedPointer);

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
		batch.spentby.apply();
		batch.tx.apply();
		batch.txid.apply();
		batch.pubkey.apply();

		return { pointer: blockPointer };
	} catch (reason) {
		batch.header.discard();
		batch.block.discard();
		batch.spentby.discard();
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

export async function geHeaderByHash(hash: Uint8Array): Promise<StoredBlockHeader | undefined> {
	const height = headerHashMap.get(hash);
	if (height === undefined) return undefined;
	return await getHeaderByHeight(height);
}

export async function getTxByPointer(pointer: StoredPointer): Promise<Tx> {
	const storedTx = await atomic.stores.tx.get(pointer, StoredTx, { readAheadSize: 400_000 });
	return Tx.fromStore(storedTx);
}

export async function getTxById(txId: Uint8Array): Promise<Tx | undefined> {
	const pointer = await atomic.stores.txid.get(txId);
	if (pointer === undefined) return undefined;
	return await getTxByPointer(pointer.tx);
}

export async function getTxsByBlockPointer(pointer: StoredPointer): Promise<Tx[] | undefined> {
	const storedTxs = await atomic.stores.tx.get(pointer, StoredTxs, { readAheadSize: MAX_BLOCK_WEIGHT });
	if (!storedTxs) return undefined;
	return await Promise.all(storedTxs.map(Tx.fromStore));
}

export async function getTxsByBlockHeight(height: number): Promise<Tx[] | undefined> {
	const pointer = await atomic.stores.block.get(height);
	if (pointer === 0 && height !== 0) return undefined;
	return await getTxsByBlockPointer(pointer);
}

export async function getTxsByBlockHash(hash: Uint8Array): Promise<Tx[] | undefined> {
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
	return header.hash;
}

export async function getTxPointerById(txId: Uint8Array): Promise<StoredPointer | undefined> {
	return (await atomic.stores.txid.get(txId))?.tx;
}

export async function getBlockPointerByHeight(height: number): Promise<StoredPointer | undefined> {
	return await atomic.stores.block.get(height);
}

export async function getBlockPointerByHash(hash: Uint8Array): Promise<StoredPointer | undefined> {
	const height = headerHashMap.get(hash);
	if (height === undefined) return undefined;
	return await getBlockPointerByHeight(height);
}

export async function getChainTip(): Promise<{ height: number; block: StoredBlockHeader } | undefined> {
	const height = atomic.stores.header.length() - 1;
	if (height < 0) return undefined;
	const block = await getHeaderByHeight(height);
	if (!block) return undefined;
	return { height, block };
}

export async function getTxOutputByPointer(pointer: number): Promise<TxOutput> {
	return await atomic.stores.tx.get(pointer, StoredTxOutput);
}

export async function getTxIdByPointer(pointer: number): Promise<Uint8Array> {
	return await atomic.stores.tx.get(pointer, Bytes32);
}
