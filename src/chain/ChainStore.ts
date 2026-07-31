import { StructCodec, U32, VarInt } from "@nomadshiba/codec";
import { join } from "@std/path";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { U48 } from "~/codec/primitives/U48.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredBlockInfo } from "~/codec/stored/StoredBlockInfo.ts";
import { StoredPubKey } from "~/codec/stored/StoredPubKey.ts";
import { StoredPubkeyPointer } from "~/codec/stored/StoredPubkeyPointer.ts";
import { StoredTxIdPointer } from "~/codec/stored/StoredTxIdPointer.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { COINBASE_TXID, GB, MINUTE } from "~/constants.ts";
import { BASE_DATA_DIR } from "~/env.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { Atomic } from "~/libs/storage/Atomic.ts";
import { BlobStore, CompressionOptions } from "~/libs/storage/BlobStore.ts";
import { GrowthOptions, HashMapStore, LoadFactorOptions } from "~/libs/storage/HashMapStore.ts";

const COMPRESSION_OPTIONS: CompressionOptions = {
	maxInflatedChunkAge: 15 * MINUTE,
	maxInflatedChunks: 8,
	zstd: {
		compress: {
			compressionLevel: 19,
			enableLongDistanceMatching: 1,
			windowLog: 27, // maybe make it 24 later?
			checksumFlag: 1, // 4-byte frame checksum, cheap integrity guard
			contentSizeFlag: 1, // size in frame header — works on the sync path,
		},
		decompress: {
			windowLogMax: 27,
		},
	},
};

const LOAD_FACTOR_OPTIONS: LoadFactorOptions = {
	target: .7,
	maxDrift: .15,
};

const GROWTH_OPTIONS: GrowthOptions = {
	amount: 1 * GB,
	headroom: 1 * GB,
};

export class ChainStore {
	public readonly atomic = Atomic.open({
		path: join(BASE_DATA_DIR, "meta"),
		stores: {
			header: ArrayStore.open({
				path: join(BASE_DATA_DIR, "header"),
				item: StoredBlockHeader,
				cursor: U40,
				minChunkSize: 1 * GB,
			}),
			block: ArrayStore.open({
				path: join(BASE_DATA_DIR, "block"),
				item: StoredBlockInfo,
				cursor: U40,
				minChunkSize: 1 * GB,
			}),
			blockhash: HashMapStore.open({
				path: join(BASE_DATA_DIR, "blockhash"),
				key: Bytes32, // block hash
				value: U32, // block height
				pointer: U40,
				loadFactor: LOAD_FACTOR_OPTIONS,
				growth: GROWTH_OPTIONS,
			}),
			tx: BlobStore.open({
				path: join(BASE_DATA_DIR, "tx"),
				cursor: StoredTxPointer,
				chunkSize: 1 * GB,
			}),
			txid: HashMapStore.open({
				path: join(BASE_DATA_DIR, "txid"),
				key: Bytes32, // tx id
				value: StoredTxPointer, // pointer to tx block store
				pointer: StoredTxIdPointer,
				loadFactor: LOAD_FACTOR_OPTIONS,
				growth: GROWTH_OPTIONS,
			}),
			pubkey: HashMapStore.open({
				path: join(BASE_DATA_DIR, "pubkey"),
				key: StoredPubKey,
				value: StoredTxIdPointer, // pointer to last tx of the pubkey at txid hashmap store
				pointer: StoredPubkeyPointer,
				loadFactor: LOAD_FACTOR_OPTIONS,
				growth: GROWTH_OPTIONS,
			}),
			spender: HashMapStore.open({
				path: join(BASE_DATA_DIR, "spender"),
				key: new StructCodec({ tx: StoredTxIdPointer, output: VarInt }),
				value: StoredTxIdPointer, // spender tx
				pointer: U48,
				loadFactor: LOAD_FACTOR_OPTIONS,
				growth: GROWTH_OPTIONS,
			}),
		},
	});
	public readonly stores = this.atomic.stores;

	private constructor() {}

	private static main_: ChainStore;
	public static main() {
		return this.main_ ??= new ChainStore();
	}

	public getPrevOutTxId(input: StoredTxInput): Uint8Array<ArrayBuffer> {
		const txId = input.prevOut.txId;
		if (txId === null) return COINBASE_TXID;
		const [key] = this.stores.txid.getKey(txId);
		return key;
	}
}

export const chainStore = ChainStore.main();
if (self.name === "chain") {
	chainStore.stores.tx.startCompression(COMPRESSION_OPTIONS);
}
