import { StructCodec, U32, VarInt, Void } from "@nomadshiba/codec";
import { join } from "@std/path";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { U48 } from "~/codec/primitives/U48.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredPubkeyCursor } from "~/codec/stored/StoredPubkeyCursor.ts";
import { StoredPubKey } from "~/codec/stored/StoredPubKey.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxCursor } from "~/codec/stored/StoredTxCursor.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { COINBASE_TXID, GB, MINUTE } from "~/constants.ts";
import { BASE_DATA_DIR } from "~/env.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { Atomic } from "~/libs/storage/Atomic.ts";
import { BlobStore, CompressionOptions } from "~/libs/storage/BlobStore.ts";
import { HashMapStore, LoadFactorOptions } from "~/libs/storage/HashMapStore.ts";

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

export class ChainStorage {
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
				item: StoredTxCursor, // cursor to txid hashmap store to first tx (txid only exists there)
				cursor: U40,
				minChunkSize: 1 * GB,
			}),
			blockhash: HashMapStore.open({
				path: join(BASE_DATA_DIR, "blockhash"),
				key: Bytes32, // block hash
				value: U32, // block height
				pointer: U40,
				loadFactor: LOAD_FACTOR_OPTIONS,
			}),
			tx: BlobStore.open({
				path: join(BASE_DATA_DIR, "tx"),
				entry: StoredTxs,
				cursor: U48,
				chunkSize: 1 * GB,
			}),
			txid: HashMapStore.open({
				path: join(BASE_DATA_DIR, "txid"),
				key: Bytes32, // tx id
				value: U48, // cursor to tx block store
				pointer: StoredTxCursor,
				loadFactor: LOAD_FACTOR_OPTIONS,
			}),
			pubkey: HashMapStore.open({
				path: join(BASE_DATA_DIR, "pubkey"),
				key: StoredPubKey,
				value: Void, // TODO: HEAD to pubkey's outputs maybe?
				pointer: StoredPubkeyCursor,
				loadFactor: LOAD_FACTOR_OPTIONS,
			}),
			spender: HashMapStore.open({
				path: join(BASE_DATA_DIR, "spender"),
				key: new StructCodec({ tx: StoredTxCursor, output: VarInt }),
				value: StoredTxCursor, // spender tx
				pointer: U48,
				loadFactor: LOAD_FACTOR_OPTIONS,
			}),
		},
	});
	public readonly stores = this.atomic.stores;

	private constructor() {}

	private static main_: ChainStorage;
	public static main() {
		return this.main_ ??= new ChainStorage();
	}

	public getPrevOutTxId(input: StoredTxInput): Uint8Array<ArrayBuffer> {
		const { kind, value } = input.prevOut.txId;
		if (kind === "pointer") {
			const [key] = this.stores.txid.getKey(value);
			return key;
		}
		if (kind === "coinbase") return COINBASE_TXID;
		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}
}

export const chainStorage = ChainStorage.main();
if (self.name === "chain") {
	chainStorage.stores.tx.startCompression(COMPRESSION_OPTIONS);
}
