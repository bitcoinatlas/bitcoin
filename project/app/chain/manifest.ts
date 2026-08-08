import { StructCodec, U32 } from "@nomadshiba/codec";
import {
	Bytes32,
	NullableNumaricCodec,
	StoredBlockHeader,
	StoredBlockInfo,
	StoredPubKey,
	StoredTxIdPointer,
	StoredTxInfo,
	StoredTxInput,
	U40,
	U48,
	WireTxInput,
} from "@project/codecs";
import { COINBASE_TXID, GB, MAX_BLOCK_SIZE, MB } from "@project/utils";
import { join } from "@std/path";
import { BASE_DATA_DIR } from "~/env.ts";
import { ArrayStore, BlobStore, HashMapStore, LoadFactorOptions, Manifest } from "~/libs/storage/mod.ts";
import { SharedArrayStore } from "~/libs/storage/SharedArrayStore.ts";

const LOAD_FACTOR_OPTIONS: LoadFactorOptions = {
	target: .75,
	maxDrift: .25,
};

const StoredOutputIndex = U40;

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
			commiter: self.name === "chain",
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
			commiter: self.name === "chain",
			entryChunkSize: 500 * MB,
			minBucketChunkSize: 500 * MB,
			pointer: StoredTxIdPointer,
		}),
		pubkey: HashMapStore.open({
			commiter: self.name === "chain",
			path: join(BASE_DATA_DIR, "pubkey"),
			loadFactor: LOAD_FACTOR_OPTIONS,
			entries: {
				key: StoredPubKey,
				value: StoredOutputIndex,
				chunkSize: 1 * GB,
				maxEntrySize: MAX_BLOCK_SIZE,
				pointer: U48,
			},
			buckets: {
				initialSize: 1_000_000,
				minChunkSize: 500 * MB,
			},
			links: {
				index: StoredOutputIndex,
				minChunkSize: 500 * MB,
			},
			sha256: true,
		}),
		output: SharedArrayStore.open({
			writable: self.name === "chain",
			path: join(BASE_DATA_DIR, "output"),
			item: new StructCodec({
				ownerTx: StoredTxIdPointer,
				spenderTx: new NullableNumaricCodec(StoredTxIdPointer),
				prevSamePubkeyOutputIndex: new NullableNumaricCodec(U40),
			}),
			minChunkSize: 500 * MB,
		}),
	},
});

export function getPrevOutTxId(input: StoredTxInput): WireTxInput["prevOut"]["txId"] {
	const txId = input.prevOut.txId;
	if (txId === null) return COINBASE_TXID;
	const [key] = manifest.stores.txid.getEntry(txId);
	return key;
}
