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

export interface Store<T extends Batch = Batch> {
	batch(): T;
	flush(): Promise<void>;
	rollback(): Promise<void>;
}
