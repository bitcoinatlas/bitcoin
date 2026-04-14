/**
 * Represents a durable transaction that can be committed or rolled back.
 *
 * Concrete implementations extend this with typed operation methods,
 * e.g. `KVStoreTransaction` adds `.set(key, value)`, `.delete(key)`, etc.
 *
 * Commit only writes a WAL to disk — no mutation happens yet.
 * Mutation is deferred to `Transactionable.finalize()`.
 */
export type Transaction = {
	/**
	 * Write all staged operations to a WAL on disk.
	 * Does not mutate the store. Safe to call per-store independently.
	 * On crash after commit but before finalize, the WAL is replayed on restart.
	 */
	commit(): Promise<void>;

	/**
	 * Discard all staged operations. Only valid before commit().
	 */
	rollback(): void;
};

export type Transactionable = {
	transaction(): Transaction | Promise<Transaction>;

	/**
	 * Apply any pending WAL and delete it. Idempotent — no-op if no WAL exists.
	 * Must be called on startup for crash recovery, and after all stores have
	 * commit()ed to make changes durable.
	 */
	finalize(): Promise<void>;
};
