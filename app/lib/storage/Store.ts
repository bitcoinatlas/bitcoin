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

/**
 * A Write-Ahead Log entry for a single store.
 *
 * Represents a durable record of pending changes that can survive a crash.
 * `apply()` is replayable — safe to call multiple times; This is required for crash recovery.
 *
 * - `save()` — write the WAL to disk. This is a separate step from `apply()` to allow for atomic flushes across multiple stores.
 * - `apply()` — replay the WAL's changes onto the store. Never fails; a failure is a bug and should panic.
 * - `discard()` — delete the WAL file if it exists.
 */
export type WAL = {
	apply(): Promise<void>;
	discard(): Promise<void>;
};

/**
 * A persistent store that supports batched writes and WAL-based crash recovery.
 *
 * - `name` — unique identifier for the store, used for tracking pending atomic flushes.
 * - `batch()` — create an in-memory batch to stage changes.
 * - `WAL()` — return the store's existing WAL if one is on disk, or null.
 * - `WAL({ create: true })` — if WAL doesn't exist return a new one.
 */
export interface Store<T extends Batch = Batch> {
	wal: WAL | null;
	createWAL(): Promise<WAL>;
	batch(): T;
}
