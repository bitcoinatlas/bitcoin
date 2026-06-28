import { RocksDatabase } from "@harperfast/rocksdb-js";

export abstract class Store {
	abstract readonly path: string;

	abstract pin(): void;
	abstract rollback(): void;
}

export type FlushFinalizer = () => void;

export abstract class StoreRocks {
	abstract readonly rocksdb: RocksDatabase;
}
