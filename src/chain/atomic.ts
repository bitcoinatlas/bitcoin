import { ArrayCodec, StructCodec } from "@nomadshiba/codec";
import { join } from "@std/path";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredPubkeyPointer } from "~/codec/stored/StoredPubkeyPointer.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { BASE_DATA_DIR } from "~/env.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { Atomic } from "~/libs/storage/Atomic.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { KvStore } from "~/libs/storage/KvStore.ts";
import { RocksDatabase, RocksDatabaseOptions } from "@harperfast/rocksdb-js";

const MiB = 1024 ** 2;
const GiB = 1024 ** 3;

const blockCacheSize = 4 * GiB; // holds ribbon filters + indexes w/ headroom
const perCfWriteBuffer = 256 * MiB; // memtable flush size, per CF
const wbmSize = 2 * GiB; // hard-ish global cap across all 6 CFs

RocksDatabase.config({
	blockCacheSize,
	writeBufferManagerSize: wbmSize,
	writeBufferManagerCostToCache: false, // keep filters/data separate from memtable budget
	writeBufferManagerAllowStall: true, // enforce the cap (choose deliberately)
});

const ROCKS_PATH = join(BASE_DATA_DIR, "rocksdb");
const ROCKS_OPTIONS: RocksDatabaseOptions = {
	writeBufferSize: perCfWriteBuffer,
	parallelismThreads: navigator.hardwareConcurrency,
	bloomBitsPerKey: 10,
	ribbonFilter: true,
	transactionLogMaxSize: 13,
};

const rocksdb = RocksDatabase.open(ROCKS_PATH, ROCKS_OPTIONS);

export const atomic = Atomic.open({
	rocksdb,
	stores: {
		headers: ArrayStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "headers" }),
			path: join(BASE_DATA_DIR, "headers"),
			codec: StoredBlockHeader,
			itemsPerChunk: 1_000_000,
		}),
		blocks: ArrayStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "blocks" }),
			path: join(BASE_DATA_DIR, "blocks"),
			codec: StoredTxPointer,
			itemsPerChunk: 1_000_000,
		}),
		txs: BlobStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "txs" }),
			path: join(BASE_DATA_DIR, "txs"),
			maxChunkSize: 1 * 1000 * 1000 * 1000,
		}),
		pubkey: KvStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "pubkey" }),
			key: Bytes32,
			// TODO: value might also include HEAD and TAIL of the linked list of outputs or something
			value: StoredPubkeyPointer,
		}),
		pubkeys: BlobStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "pubkeys" }),
			path: join(BASE_DATA_DIR, "pubkeys"),
			maxChunkSize: 1 * 1000 * 1000 * 1000,
		}),
		txid: KvStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "txid" }),
			key: Bytes32,
			value: new StructCodec({
				pointer: StoredTxPointer,
				spenders: new ArrayCodec(StoredTxPointer),
			}),
		}),
	},
});
