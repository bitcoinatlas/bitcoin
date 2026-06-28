import { join } from "@std/path";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";
import { BASE_DATA_DIR } from "~/env.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { Atomic } from "~/libs/storage/Atomic.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { KvStore } from "~/libs/storage/KvStore.ts";
import { RocksDatabase } from "../../vendor/rocksdb-js/dist/index.d.cts";
import { U40 } from "~/codec/primitives/U40.ts";
import { StructCodec } from "@nomadshiba/codec";

const ROCKS_PATH = join(BASE_DATA_DIR, "rocksdb");

const GiB = 1024 ** 3;
const totalRam = Deno.systemMemoryInfo().total;

// Reserve for OS + page cache + V8 + other processes. Scale the reserve with
// total RAM (a 64 GiB box can spare proportionally more than an 8 GiB one).
const osReserve = Math.max(2 * GiB, totalRam * 0.25);

const writeBufferSize = 2 * GiB; // additive (costToCache:false)

// Block cache gets a slice of what remains, clamped to a sane band.
const available = totalRam - osReserve - writeBufferSize;
const blockCacheSize = Math.max(
	1 * GiB, // floor — below this rocksdb thrashes
	Math.min(available * 0.6, 32 * GiB), // 60% of remainder, hard ceiling
);

RocksDatabase.config({
	blockCacheSize,
	writeBufferManagerSize: writeBufferSize,
	writeBufferManagerCostToCache: false,
	writeBufferManagerAllowStall: false,
});

const rocksdb = new RocksDatabase(ROCKS_PATH, {
	disableWAL: true,
	pessimistic: true,
	parallelismThreads: Math.min(10, navigator.hardwareConcurrency),
	transactionLogRetention: 0,
	bloomBitsPerKey: 10,
	ribbonFilter: true,
	transactionLogMaxSize: 0,
});

export const atomic = Atomic.open({
	rocksdb,
	path: join(BASE_DATA_DIR, "atomic"),
	stores: {
		headers: ArrayStore.open({
			path: join(BASE_DATA_DIR, "headers"),
			codec: StoredBlockHeader,
			itemsPerChunk: 1_000_000,
		}),
		blocks: ArrayStore.open({
			path: join(BASE_DATA_DIR, "blocks"),
			codec: StoredPointer,
			itemsPerChunk: 1_000_000,
		}),
		txs: BlobStore.open({
			path: join(BASE_DATA_DIR, "txs"),
			maxChunkSize: 1 * 1000 * 1000 * 1000,
		}),
		pubkey: KvStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { name: "pubkey" }),
			key: Bytes32,
			// TODO: value might also include HEAD and TAIL of the linked list of outputs or something
			value: new StructCodec({ pointer: U40 }),
		}),
		pubkeys: BlobStore.open({
			path: join(BASE_DATA_DIR, "pubkeys"),
			maxChunkSize: 1 * 1000 * 1000 * 1000,
		}),
		txid: KvStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { name: "txid" }),
			key: Bytes32,
			value: StoredPointer,
		}),
	},
});
