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
import { COINBASE_TXID, GB, MB } from "~/constants.ts";
import { BASE_DATA_DIR } from "~/env.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { Manifest } from "~/libs/storage/Manifest.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { HashMapStore, LoadFactorOptions } from "~/libs/storage/HashMapStore.ts";

const LOAD_FACTOR_OPTIONS: LoadFactorOptions = {
	target: .7,
	maxDrift: .15,
};

export class ChainStore {
	public readonly atomicSharedArrayBuffer: SharedArrayBuffer;
	public readonly atomic = Manifest.open({
		path: join(BASE_DATA_DIR, "meta"),
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
				writable: self.name === "p2p",
				blob: {
					chunkSize: 500 * MB,
				},
			}),
			tx: BlobStore.open({
				path: join(BASE_DATA_DIR, "tx"),
				chunkSize: 1 * GB,
			}),
			txid: HashMapStore.open({
				path: join(BASE_DATA_DIR, "txid"),
				key: Bytes32, // tx id
				value: StoredTxPointer, // pointer to tx block store
				pointer: StoredTxIdPointer,
				loadFactor: LOAD_FACTOR_OPTIONS,
				writable: self.name === "chain",
			}),
			pubkey: HashMapStore.open({
				path: join(BASE_DATA_DIR, "pubkey"),
				key: StoredPubKey,
				value: StoredTxIdPointer, // pointer to last tx of the pubkey at txid hashmap store
				pointer: StoredPubkeyPointer,
				loadFactor: LOAD_FACTOR_OPTIONS,
				writable: self.name === "chain",
			}),
			spender: HashMapStore.open({
				path: join(BASE_DATA_DIR, "spender"),
				key: new StructCodec({ tx: StoredTxIdPointer, output: VarInt }),
				value: StoredTxIdPointer, // spender tx
				pointer: U48,
				loadFactor: LOAD_FACTOR_OPTIONS,
				writable: self.name === "chain",
			}),
		},
	});
	public readonly stores = this.atomic.stores;

	private constructor(atomicSharedArrayBuffer?: SharedArrayBuffer) {
		this.atomicSharedArrayBuffer = atomicSharedArrayBuffer ?? new SharedArrayBuffer();
	}

	public static open(atomicSharedArrayBuffer?: SharedArrayBuffer) {
		return new ChainStore(atomicSharedArrayBuffer);
	}

	public getPrevOutTxId(input: StoredTxInput): Uint8Array<ArrayBuffer> {
		const txId = input.prevOut.txId;
		if (txId === null) return COINBASE_TXID;
		const [key] = this.stores.txid.getKey(txId);
		return key;
	}
}

export const chainStore = ChainStore.open();
