import { RocksDatabase, RocksDatabaseOptions } from "@harperfast/rocksdb-js";
import { StructCodec, U32 } from "@nomadshiba/codec";
import { join } from "@std/path";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredPubkeyPointer } from "~/codec/stored/StoredPubkeyPointer.ts";
import { StoredScriptPubKey } from "~/codec/stored/StoredScriptPubkey.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { COINBASE_TXID, GB, GiB, MAX_BLOCK_WEIGHT, MiB, MINUTE } from "~/constants.ts";
import { BASE_DATA_DIR, PARALLELISM_THREADS } from "~/env.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { Atomic } from "~/libs/storage/Atomic.ts";
import { BlobStore, CompressionOptions } from "~/libs/storage/BlobStore.ts";
import { KvStore } from "~/libs/storage/KvStore.ts";

// TODO: Later blockCacheSize can be calculated by device memory and core count and etc. idk
// NOTE: If there is a huge slow down check blockCacheSize and try to make it bigger
RocksDatabase.config({
	blockCacheSize: 750 * MiB, // mul by PARALLELISM to find how much memory this uses in total
	writeBufferManagerSize: 1 * GiB, // global i think
});

const ROCKS_PATH = join(BASE_DATA_DIR, "indexes");
const ROCKS_OPTIONS: RocksDatabaseOptions = {
	disableWAL: true,
	parallelismThreads: PARALLELISM_THREADS,
	bloomBitsPerKey: 10,
	ribbonFilter: true,
};

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

export class ChainStorage {
	public readonly atomic = Atomic.open({
		rocksdb: RocksDatabase.open(ROCKS_PATH, ROCKS_OPTIONS),
		stores: {
			headers: ArrayStore.open({
				rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "headers" }),
				path: join(BASE_DATA_DIR, "headers"),
				codec: StoredBlockHeader,
				itemsPerChunk: 1_000_000,
				writable: self.name === "chain",
			}),
			blocks: ArrayStore.open({
				rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "blocks" }),
				path: join(BASE_DATA_DIR, "blocks"),
				codec: StoredTxPointer,
				itemsPerChunk: 1_000_000,
				writable: self.name === "chain",
			}),
			txs: BlobStore.open({
				rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "txs" }),
				path: join(BASE_DATA_DIR, "txs"),
				maxChunkSize: 1 * GB,
				writable: self.name === "chain",
				compression: COMPRESSION_OPTIONS,
			}),
			txid: KvStore.open({
				rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "txid" }),
				key: Bytes32,
				value: StoredTxPointer,
			}),
			pubkeys: BlobStore.open({
				rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "pubkeys" }),
				path: join(BASE_DATA_DIR, "pubkeys"),
				maxChunkSize: 1 * GB,
				writable: self.name === "chain",
			}),
			pubkey: KvStore.open({
				rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "pubkey" }),
				key: Bytes32,
				value: StoredPubkeyPointer, // TODO: value might also include HEAD and TAIL of the linked list of outputs or something
			}),
			spenders: KvStore.open({
				rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "spenders" }),
				key: new StructCodec({ tx: StoredTxPointer, output: U32 }), // output // TODO: make output VarInt
				value: StoredTxPointer, // spender tx
			}),
		},
	});
	public readonly stores = this.atomic.stores;
	public readonly rocksdb = this.atomic.rocksdb;
	private readonly hashIndex: FastUint8ArrayMap<number>;
	private readonly indexHeightByHashChannel: BroadcastChannel;

	private constructor() {
		const length = this.stores.headers.length();
		const index = new FastUint8ArrayMap<number>(Math.max(256, length * 2));
		const STEP = 100_000;
		for (let from = 0; from < length; from += STEP) {
			const to = Math.min(length, from + STEP);
			const headers = this.stores.headers.slice(from, to);
			for (let i = 0; i < headers.length; i++) index.set(headers[i]!.hash(), from + i);
		}
		this.hashIndex = index;
		this.indexHeightByHashChannel = new BroadcastChannel("ChainStore.indexHeightByHash");
		this.indexHeightByHashChannel.addEventListener("message", (event) => {
			const { hash, height } = event.data;
			this.hashIndex.set(hash, height);
		});
	}

	private static main_: ChainStorage;
	public static main() {
		return this.main_ ??= new ChainStorage();
	}

	public getHeightByHash(hash: Uint8Array): number | undefined {
		return this.hashIndex.get(hash);
	}

	public indexHeightByHash(hash: Uint8Array, height: number) {
		this.hashIndex.set(hash, height);
		this.indexHeightByHashChannel.postMessage({ hash, height });
	}

	public getHeaderByHeight(height: number): StoredBlockHeader | undefined {
		if (height < 0 || height >= this.stores.headers.length()) return undefined;
		return this.stores.headers.get(height);
	}

	public async getHeaderByHeightAsync(height: number): Promise<StoredBlockHeader | undefined> {
		if (height < 0 || height >= this.stores.headers.length()) return undefined;
		return this.stores.headers.getAsync(height);
	}

	public getHeaderByRange(from: number, to: number): Array<{ height: number; header: WireBlockHeader }> {
		const length = this.stores.headers.length();
		const lo = Math.max(0, from);
		const hi = Math.min(length - 1, to);
		if (hi < lo) return [];
		const headers = this.stores.headers.slice(lo, hi + 1);
		return headers.map((header, i) => ({ height: lo + i, header }));
	}

	public async getHeaderByRangeAsync(from: number, to: number): Promise<Array<{ height: number; header: WireBlockHeader }>> {
		const length = this.stores.headers.length();
		const lo = Math.max(0, from);
		const hi = Math.min(length - 1, to);
		if (hi < lo) return [];
		const headers = await this.stores.headers.sliceAsync(lo, hi + 1);
		return headers.map((header, i) => ({ height: lo + i, header }));
	}

	public getChainTip(): { height: number; header: StoredBlockHeader } | undefined {
		const height = this.stores.headers.length() - 1;
		if (height < 0) return undefined;
		const header = this.getHeaderByHeight(height);
		if (!header) return undefined;
		return { height, header };
	}

	public async getChainTipAsync(): Promise<{ height: number; header: StoredBlockHeader } | undefined> {
		const height = this.stores.headers.length() - 1;
		if (height < 0) return undefined;
		const header = await this.getHeaderByHeightAsync(height);
		if (!header) return undefined;
		return { height, header };
	}

	public getTxsByBlockHeight(height: number): StoredTx[] | undefined {
		const pointer = height === 0 ? 0 : this.stores.blocks.get(height);
		if (pointer === undefined) return undefined;
		return this.stores.txs.get(pointer, StoredTxs, { readAheadSize: MAX_BLOCK_WEIGHT });
	}

	public async getTxsByBlockHeightAsync(height: number): Promise<StoredTx[] | undefined> {
		const pointer = height === 0 ? 0 : await this.stores.blocks.getAsync(height);
		if (pointer === undefined) return undefined;
		return this.stores.txs.getAsync(pointer, StoredTxs, { readAheadSize: MAX_BLOCK_WEIGHT });
	}

	public getTxById(txId: Uint8Array): StoredTx | undefined {
		const pointer = this.stores.txid.get(txId);
		if (pointer === undefined) return undefined;
		return this.stores.txs.get(pointer, StoredTx, { readAheadSize: 400_000 });
	}

	public async getTxByIdAsync(txId: Uint8Array): Promise<StoredTx | undefined> {
		const pointer = await this.stores.txid.getAsync(txId);
		if (pointer === undefined) return undefined;
		return this.stores.txs.getAsync(pointer, StoredTx, { readAheadSize: 400_000 });
	}

	public getTxByPointer(pointer: StoredTxPointer): StoredTx {
		return this.stores.txs.get(pointer, StoredTx, { readAheadSize: 400_000 });
	}

	public async getTxByPointerAsync(pointer: StoredTxPointer): Promise<StoredTx> {
		return this.stores.txs.getAsync(pointer, StoredTx, { readAheadSize: 400_000 });
	}

	public getPrevOutTxId(input: StoredTxInput): Uint8Array {
		const { kind, value } = input.prevOut.txId;
		if (kind === "pointer") return this.getTxByPointer(value).txId;
		if (kind === "coinbase") return COINBASE_TXID;
		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}

	public async getPrevOutTxIdAsync(input: StoredTxInput): Promise<Uint8Array> {
		const { kind, value } = input.prevOut.txId;
		if (kind === "pointer") return (await this.getTxByPointerAsync(value)).txId;
		if (kind === "coinbase") return COINBASE_TXID;
		throw new Error(`getPrevOutTxId doesn't handle txId kind: ${kind satisfies never}`);
	}

	public getScriptPubKeyFromPointer(pointer: number): StoredScriptPubKey {
		return this.stores.pubkeys.get(pointer, StoredScriptPubKey);
	}

	public async getScriptPubKeyFromPointerAsync(pointer: number): Promise<StoredScriptPubKey> {
		return this.stores.pubkeys.getAsync(pointer, StoredScriptPubKey);
	}
}
export const chainStorage = ChainStorage.main();
