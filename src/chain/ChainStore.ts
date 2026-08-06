import { StructCodec, U32 } from "@nomadshiba/codec";
import { join } from "@std/path";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredBlockInfo } from "~/codec/stored/StoredBlockInfo.ts";
import { StoredPubKey } from "~/codec/stored/StoredPubKey.ts";
import { StoredTxIdPointer } from "~/codec/stored/StoredTxIdPointer.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { COINBASE_TXID, GB, MAX_BLOCK_SIZE, MB } from "~/constants.ts";
import { BASE_DATA_DIR } from "~/env.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { HashMapStore, LoadFactorOptions } from "~/libs/storage/HashMapStore.ts";
import { Manifest } from "~/libs/storage/Manifest.ts";

const LOAD_FACTOR_OPTIONS: LoadFactorOptions = {
	target: .75,
	maxDrift: .25,
};

export class ChainStore {
	public readonly manifest = Manifest.open({
		path: join(BASE_DATA_DIR, "manifest"),
		pinner: self.name === "",
		stores: {
			header: ArrayStore.open({
				path: join(BASE_DATA_DIR, "header"),
				item: StoredBlockHeader,
				minChunkSize: 1 * GB,
			}),
			block: ArrayStore.open({
				path: join(BASE_DATA_DIR, "block"),
				item: StoredBlockInfo,
				minChunkSize: 1 * GB,
			}),
			blockhash: HashMapStore.open({
				path: join(BASE_DATA_DIR, "blockhash"),
				key: Bytes32, // block hash
				value: U32, // block height
				loadFactor: LOAD_FACTOR_OPTIONS,
				commiter: self.name === "chain" || self.name === "",
				entryChunkSize: 500 * MB,
				minBucketChunkSize: 500 * MB,
			}),
			tx: BlobStore.open({
				path: join(BASE_DATA_DIR, "tx"),
				chunkSize: 1 * GB,
				restore: { windowLogMax: 27 }, // must cover the archive's windowLog (27)
			}),
			txid: HashMapStore.open({
				path: join(BASE_DATA_DIR, "txid"),
				key: Bytes32, // tx id
				value: new StructCodec({ txPointer: StoredTxPointer, totalOutput: U40 }),
				loadFactor: LOAD_FACTOR_OPTIONS,
				commiter: self.name === "chain" || self.name === "",
				entryChunkSize: 500 * MB,
				minBucketChunkSize: 500 * MB,
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
			}),
			spender: ArrayStore.open({
				path: join(BASE_DATA_DIR, "spender"),
				item: StoredTxIdPointer,
				minChunkSize: 500 * MB,
			}),
		},
	});
	public readonly stores = this.manifest.stores;

	private constructor() {}

	public static open() {
		return new ChainStore();
	}

	public getPrevOutTxId(input: StoredTxInput): Uint8Array<ArrayBuffer> {
		const txId = input.prevOut.txId;
		if (txId === null) return COINBASE_TXID;
		const [key] = this.stores.txid.getKey(txId);
		return key;
	}
}

export const chainStore = ChainStore.open();
