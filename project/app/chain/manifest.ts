import { U32 } from "@nomadshiba/codec";
import {
	Bytes32,
	StoredBlockHeader,
	StoredBlockInfo,
	StoredPubKey,
	StoredTxIdPointer,
	StoredTxInfo,
	StoredTxInput,
	U40,
	U48,
} from "@project/codecs";
import { COINBASE_TXID, GB, MAX_BLOCK_SIZE, MB } from "@project/utils";
import { join } from "@std/path";
import { BASE_DATA_DIR } from "~/env.ts";
import { ArrayStore, BlobStore, HashMapStore, LoadFactorOptions, Manifest } from "~/libs/storage/mod.ts";

const LOAD_FACTOR_OPTIONS: LoadFactorOptions = {
	target: .75,
	maxDrift: .25,
};

export const manifest = Manifest.open({
	path: join(BASE_DATA_DIR, "manifest"),
	pinner: self.name === "",
	stores: {
		header: ArrayStore.open({
			path: join(BASE_DATA_DIR, "header"),
			item: StoredBlockHeader,
			minChunkSize: 1 * GB,
		}),
		headerhash: HashMapStore.open({
			path: join(BASE_DATA_DIR, "headerhash"),
			key: Bytes32, // block hash
			value: U32, // block height
			loadFactor: LOAD_FACTOR_OPTIONS,
			commiter: self.name === "chain" || self.name === "",
			entryChunkSize: 500 * MB,
			minBucketChunkSize: 500 * MB,
			pointer: U40,
		}),
		block: ArrayStore.open({
			path: join(BASE_DATA_DIR, "block"),
			item: StoredBlockInfo,
			minChunkSize: 1 * GB,
		}),
		tx: BlobStore.open({
			path: join(BASE_DATA_DIR, "tx"),
			chunkSize: 1 * GB,
			restore: { windowLogMax: 27 }, // must cover the archive's windowLog (27)
		}),
		txid: HashMapStore.open({
			path: join(BASE_DATA_DIR, "txid"),
			key: Bytes32, // tx id
			value: StoredTxInfo,
			loadFactor: LOAD_FACTOR_OPTIONS,
			commiter: self.name === "chain" || self.name === "",
			entryChunkSize: 500 * MB,
			minBucketChunkSize: 500 * MB,
			pointer: U48,
		}),
		pubkey: HashMapStore.open({
			path: join(BASE_DATA_DIR, "pubkey"),
			key: StoredPubKey,
			value: StoredTxIdPointer, // pointer to last tx of the pubkey at txid hashmap store
			loadFactor: LOAD_FACTOR_OPTIONS,
			commiter: self.name === "chain" || self.name === "",
			entryChunkSize: 500 * MB,
			minBucketChunkSize: 500 * MB,
			maxEntrySize: MAX_BLOCK_SIZE + StoredTxIdPointer.stride.size,
			pointer: U48,
		}),
		spender: ArrayStore.open({
			path: join(BASE_DATA_DIR, "spender"),
			item: StoredTxIdPointer,
			minChunkSize: 500 * MB,
		}),
	},
});

export function getPrevOutTxId(input: StoredTxInput): Uint8Array<ArrayBuffer> {
	const txId = input.prevOut.txId;
	if (txId === null) return COINBASE_TXID;
	const [key] = manifest.stores.txid.getKey(txId);
	return key;
}
