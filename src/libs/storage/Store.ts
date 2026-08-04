export abstract class Store {
	/** size or length or count etc */
	abstract size(): number;
	/**
	 * reveal the underlying storage by moving cursors.
	 *
	 * `isBroadcast` (default false) tells the store the reveal came from the
	 * broadcast/recover path rather than a manual local call. When it came from
	 * broadcast the revealed region is already persisted into the shared
	 * structures, so a store like HashMapStore skips building its in-memory stage;
	 * when called manually the store stages the region so this worker can read its
	 * own not-yet-persisted writes.
	 */
	abstract reveal(size: number, isBroadcast?: boolean): void;
	/** twin of reveal, called during pin() only instead of reveal, on the worker that said pin() */
	abstract persist(size: number): void;
	/** truncate the storage to a specific size */
	abstract truncate(size: number): void;
	/** msync or fsync */
	abstract sync(): void;
}
