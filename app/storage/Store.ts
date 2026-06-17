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
	/**
	 * Synchronously snapshot the current staged state into a frozen layer and
	 * install a fresh staged layer in its place.
	 *
	 * MUST NOT await and MUST NOT touch disk. The no-await guarantee is what lets
	 * Atomic freeze every store at a single synchronous instant with no apply()
	 * interleaving, so all stores capture the exact same logical height. Any
	 * batch applied after freeze() lands in the fresh staged layer and belongs to
	 * the next flush, not this one.
	 *
	 * Idempotent for an already-frozen store (no-op until the pending flush clears
	 * the frozen layer), so a standalone flush()/pin() may call it lazily.
	 */
	abstract freeze(): void;
	abstract pin(): Promise<void>;
	abstract flush(): Promise<void>;
	abstract rollback(): Promise<void>;
	/**
	 * Delete any rollback/WAL files written by {@link pin}.
	 *
	 * Must be called only after every store in the atomic group has flushed
	 * successfully AND the RocksDB commit has landed — i.e. from Atomic just
	 * before writing end.id. Until that point the files must stay intact so
	 * that {@link rollback} can undo a partial flush on recovery.
	 */
	abstract finalize(): Promise<void>;
}

export type FlushFinalizer = () => void;

export abstract class StoreRocks<T extends Batch = Batch> {
	abstract readonly rocksdb: RocksDatabase;
	abstract batch(): T;
	/** See {@link Store.freeze}. Same contract, for rocks-backed stores. */
	abstract freeze(): void;
	abstract flush(trx: Transaction): Promise<FlushFinalizer>;
}
