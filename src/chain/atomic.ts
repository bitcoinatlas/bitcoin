import { RocksDatabase, RocksDatabaseOptions } from "@harperfast/rocksdb-js";
import { StructCodec, U32 } from "@nomadshiba/codec";
import { join } from "@std/path";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { StoredBlockHeader } from "~/codec/stored/StoredBlockHeader.ts";
import { StoredPubkeyPointer } from "~/codec/stored/StoredPubkeyPointer.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { GiB, MiB, PARALLELISM } from "~/constants.ts";
import { BASE_DATA_DIR } from "~/env.ts";
import { ArrayStore } from "~/libs/storage/ArrayStore.ts";
import { Atomic } from "~/libs/storage/Atomic.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { KvStore } from "~/libs/storage/KvStore.ts";

// TODO: Later blockCacheSize can be calculated by device memory and core count and etc. idk
// NOTE: If there is a huge slow down check blockCacheSize and try to make it bigger
RocksDatabase.config({
	blockCacheSize: 750 * MiB, // mul by PARALLELISM to find how much memory this uses in total
	writeBufferManagerSize: 1 * GiB, // global i think
});

const ROCKS_PATH = join(BASE_DATA_DIR, "rocksdb");
const ROCKS_OPTIONS: RocksDatabaseOptions = {
	disableWAL: true,
	parallelismThreads: PARALLELISM,
	bloomBitsPerKey: 10,
	ribbonFilter: true,
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
		pubkeys: BlobStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "pubkeys" }),
			path: join(BASE_DATA_DIR, "pubkeys"),
			maxChunkSize: 1 * 1000 * 1000 * 1000,
		}),
		pubkey: KvStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "pubkey" }),
			key: Bytes32,
			// TODO: value might also include HEAD and TAIL of the linked list of outputs or something
			value: StoredPubkeyPointer,
		}),
		txid: KvStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "txid" }),
			key: Bytes32,
			value: StoredTxPointer,
		}),
		spenders: KvStore.open({
			rocksdb: RocksDatabase.open(ROCKS_PATH, { ...ROCKS_OPTIONS, name: "spenders" }),
			key: new StructCodec({ tx: StoredTxPointer, vout: U32 }), // output
			value: StoredTxPointer, // spender tx
		}),
	},
});
