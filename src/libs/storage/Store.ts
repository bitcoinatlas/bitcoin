export abstract class Store {
	/** size or length or count etc */
	abstract size(): number;
	/** reveal the underlying storage by moving cursors */
	abstract reveal(size: number): void;
	/** truncate the storage to a specific size */
	abstract truncate(size: number): void;
	/** msync or fsync */
	abstract sync(): void;
}
