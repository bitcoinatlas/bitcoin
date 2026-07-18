import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";

export abstract class Store {
	abstract readonly rocksdb: RocksDatabase;
}

export abstract class StoreAppendOnly extends Store {
	abstract pin(transaction?: Transaction): void;
	abstract rollback(transaction?: Transaction): void;
}
