import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";

export abstract class Store {
	abstract readonly path: string;
	abstract readonly rocksdb: RocksDatabase;

	abstract pin(transaction?: Transaction): void;
	abstract rollback(transaction?: Transaction): void;
}

export type FlushFinalizer = () => void;

export abstract class StoreRocks {
	abstract readonly rocksdb: RocksDatabase;
}
