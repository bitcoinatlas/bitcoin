export abstract class Store {
	abstract isReadOnly(): boolean;
	/** size or length or count etc */
	abstract size(): number;
	/** reveal the underlying storage by moving cursors */
	abstract reveal(size: number): void;
	/** truncate the storage to a specific size */
	abstract truncate(size: number): void;
	/**
	 * get the next available position for writing
	 *
	 * useful for things like `BlobStore` where you might have to skip to the next chunk boundary
	 */
	abstract next(maxSize: number, offset?: number): number;
	/** msync or fsync */
	abstract sync(): void;
}
