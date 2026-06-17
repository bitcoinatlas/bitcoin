import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";

/**
 * An in-memory batch of writes for a store.
 *
 * Enables "all or none" writes: stage changes in memory, then either
 * commit them to disk via a WAL or throw them away entirely.
 *
 * - `apply()` — flush staged changes to disk. Never fails; a failure is a bug and should panic.
 * - `discard()` — throw away staged changes. Like nothing happened.
 */
export interface Batch {
	apply(): void;
	discard(): void;
}

export abstract class Store<T extends Batch = Batch> {
	abstract readonly path: string;
	abstract batch(): T;
	abstract pin(): Promise<void>;
	abstract flush(): Promise<void>;
	abstract rollback(): Promise<void>;
}

export type FlushFinalizer = () => void;

export abstract class StoreRocks<T extends Batch = Batch> {
	abstract readonly rocksdb: RocksDatabase;
	abstract batch(): T;
	abstract flush(trx: Transaction): Promise<FlushFinalizer>;
}
