import { Codec, U64 } from "@nomadshiba/codec";
import { existsSync } from "@std/fs";
import { join } from "@std/path";
import { MAX_BLOCK_SIZE } from "~/constants.ts";
import { readFileIntoSync, writeFileSync } from "~/libs/fs/mod.ts";
import { Batch, Store } from "~/libs/storage/Store.ts";

type Region = {
	size: number;
	append(bytes: Uint8Array): void;
	readInto(offset: number, length: number, target: Uint8Array): void;
};

type MemoryRegionOptions = {
	maxChunkSize: number;
};

class MemoryRegion implements Region {
	public readonly maxChunkSize: number;

	public size: number;
	public readonly chunks: Uint8Array<ArrayBuffer>[];

	constructor(options: MemoryRegionOptions) {
		this.maxChunkSize = options.maxChunkSize;
		this.size = 0;
		this.chunks = [];
	}

	append(bytes: Uint8Array): void {
		let tailChunk = this.chunks.at(-1);
		if (!tailChunk) {
			this.chunks.push(tailChunk = new Uint8Array(new ArrayBuffer(0, { maxByteLength: this.maxChunkSize })));
		}

		let offset = 0;
		while (offset < bytes.length) {
			let available = tailChunk.buffer.maxByteLength - tailChunk.length;
			if (available === 0) {
				this.chunks.push(tailChunk = new Uint8Array(new ArrayBuffer(0, { maxByteLength: this.maxChunkSize })));
				available = tailChunk.buffer.maxByteLength;
			}

			const left = bytes.length - offset;
			const give = Math.min(left, available);

			const start = tailChunk.length;
			tailChunk.buffer.resize(tailChunk.length + give);
			tailChunk.set(bytes.subarray(offset, offset + give), start);
			offset += give;
		}
		this.size += offset;
	}

	readInto(offset: number, length: number, target: Uint8Array) {
		if (offset < 0 || length < 0) {
			throw new RangeError(`negative offset/length: offset=${offset} length=${length}`);
		}
		if (offset + length > this.size) {
			throw new RangeError(`read out of bounds: offset=${offset} length=${length} size=${this.size}`);
		}

		let chunkIndex = Math.floor(offset / this.maxChunkSize);
		let chunkOffset = offset % this.maxChunkSize;
		let outputOffset = 0;
		while (outputOffset < length) {
			const chunk = this.chunks[chunkIndex];
			if (!chunk) {
				throw new Error(`Yo no chunk 🍣. index=${chunkIndex}`);
			}

			const start = chunkOffset;
			const want = length - outputOffset;
			const end = Math.min(start + want, chunk.length);

			target.set(chunk.subarray(start, end), outputOffset);
			const got = end - start;
			if (got === 0) {
				throw new Error("Bro WHAT?! got literal 0 bytes ;-; ~~");
			}
			outputOffset += got;
			chunkIndex++;
			chunkOffset = 0;
		}
	}
}

type DiskRegionOptions = {
	path: string;
	maxChunkSize: number;
};

class DiskRegion implements Region, Disposable {
	public readonly path: string;
	public readonly maxChunkSize: number;

	public size: number;
	chunkPathCache: string[] = [];
	private chunkPath(index: number) {
		return this.chunkPathCache[index] ??= join(this.path, `chunk_${index}`);
	}

	private constructor(options: DiskRegionOptions) {
		this.path = options.path;
		this.maxChunkSize = options.maxChunkSize;
		this.size = 0;
	}

	static open(options: DiskRegionOptions): DiskRegion {
		const self = new DiskRegion(options);
		Deno.mkdirSync(self.path, { recursive: true });
		const files = Deno.readDirSync(self.path);

		let tailIndex = 0;
		const indexSet = new Set<number>([0]);
		for (const file of files) {
			if (!file.isFile) continue;
			if (!file.name.startsWith("chunk_")) continue;
			const index = Number(file.name.slice("chunk_".length));
			if (!Number.isInteger(index)) continue;
			indexSet.add(index);
			if (index > tailIndex) tailIndex = index;
		}

		for (let index = 0; index < tailIndex; index++) {
			const exist = indexSet.has(index);
			if (!exist) {
				throw new Error("bro your chunks are fucked, has some    gaps   and stuff");
			}
			const chunkStat = Deno.statSync(self.chunkPath(index));
			if (chunkStat.size !== self.maxChunkSize) {
				throw new Error(`chunk ${index} has a weird size size=${chunkStat.size}`);
			}
		}
		let tailChunkSize = 0;
		try {
			const tailChunkStat = Deno.statSync(self.chunkPath(tailIndex));
			if (tailChunkStat.size > self.maxChunkSize) {
				throw new Error(`your tail chunk is fat... size=${tailChunkStat.size}`);
			}
			tailChunkSize = tailChunkStat.size;
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
		}
		self.appender = {
			index: tailIndex,
			file: Deno.openSync(self.chunkPath(tailIndex), { create: true, append: true }),
		};
		self.size = tailIndex * self.maxChunkSize + tailChunkSize;

		return self;
	}

	private appender!: { file: Deno.FsFile; index: number };
	private appending = false;
	append(bytes: Uint8Array): void {
		if (this.appending) throw new Error("you are trying to append back to back, check your logic");
		if (this.truncating) throw new Error("you are trying to append while truncating, check your logic");
		this.appending = true;
		try {
			let size = this.size;
			let appended = 0;
			while (appended < bytes.length) {
				const want = bytes.length - appended;
				const index = Math.floor(size / this.maxChunkSize);
				const taken = size % this.maxChunkSize;
				const available = this.maxChunkSize - taken;
				const append = Math.min(want, available);

				if (this.appender.index !== index) {
					const file = Deno.openSync(this.chunkPath(index), { create: true, append: true });
					this.appender.file.close();
					this.appender.file = file;
					this.appender.index = index;
				}

				writeFileSync(this.appender.file, bytes.subarray(appended, appended + append));
				this.appender.file.syncSync();
				appended += append;
				size += append;
			}
			this.size = size;
		} finally {
			this.appending = false;
		}
	}

	readInto(offset: number, length: number, target: Uint8Array): void {
		if (offset >= this.size) {
			throw new Error(`yeah you wanna read from offset=${offset}, but all i have is size=${this.size}`);
		}

		let copied = 0;
		while (copied < length) {
			const want = length - copied;
			const index = Math.floor(offset / this.maxChunkSize);
			const start = offset % this.maxChunkSize;
			const available = this.maxChunkSize - start;
			const read = Math.min(want, available);

			using reader = Deno.openSync(this.chunkPath(index), { read: true });
			reader.seekSync(start, Deno.SeekMode.Start);
			readFileIntoSync(reader, target.subarray(copied, copied + read));

			copied += read;
			offset += read;
		}
	}

	private truncating: boolean = false;
	truncate(size: number): void {
		if (this.appending) throw new Error("you cant truncate while appending");
		if (this.truncating) throw new Error("you are trying to truncate back to back, check your logic");
		this.truncating = true;

		const oldTail = this.appender.index;
		const newTail = Math.floor(size / this.maxChunkSize);

		this.appender.file.close();
		this.appender.index = -1;

		for (let index = newTail + 1; index <= oldTail; index++) {
			Deno.removeSync(this.chunkPath(index));
		}

		const tailEnd = size % this.maxChunkSize;
		Deno.truncateSync(this.chunkPath(newTail), tailEnd);

		this.appender.file = Deno.openSync(this.chunkPath(newTail), { create: true, append: true });
		this.appender.index = newTail;

		this.size = size;

		// Should never fail to this point.
		// If it did data is corrupted
		// So that's why we are not doing try/catch
		this.truncating = false;
	}

	close() {
		this.appender.file.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}

export interface BlobStoreBatch extends Batch {
	append(data: Uint8Array): number;
	get<T extends Codec<any>>(
		pointer: number,
		codec: T,
		options?: { readAheadSize?: number },
	): Codec.InferOutput<T>;
	size(): number;
}

export type BlobStoreOptions = {
	path: string;
	maxDiskChunkSize: number;
	maxMemoryChunkSize: number;
};

export class BlobStore extends Store<BlobStoreBatch> implements Disposable {
	public readonly path: string;
	public readonly maxDiskChunkSize: number;
	public readonly maxMemoryChunkSize: number;

	private disk!: DiskRegion;
	private staged!: MemoryRegion;
	private frozen: MemoryRegion | null | undefined;
	private pendingBatch: MemoryRegion | null | undefined;
	private realizedDiskSize: number;

	private flushing = false;
	private truncating = false;
	private rollbackPath: string;

	private constructor(options: BlobStoreOptions) {
		super();
		this.path = options.path;
		this.maxDiskChunkSize = options.maxDiskChunkSize;
		this.maxMemoryChunkSize = options.maxMemoryChunkSize;

		this.realizedDiskSize = 0;
		this.rollbackPath = join(this.path, `rollback.size`);
	}

	static open(options: BlobStoreOptions): BlobStore {
		const { path } = options;
		const maxDiskChunkSize = options.maxDiskChunkSize;
		const maxMemoryChunkSize = options.maxMemoryChunkSize;

		const store = new BlobStore(options);
		store.disk = DiskRegion.open({ path, maxChunkSize: maxDiskChunkSize });
		store.staged = new MemoryRegion({ maxChunkSize: maxMemoryChunkSize });
		store.realizedDiskSize = store.disk.size;

		return store;
	}

	private *_regions(includeBatch: boolean): Generator<Region> {
		yield this.disk;
		if (this.frozen) yield this.frozen;
		yield this.staged;
		if (includeBatch && this.pendingBatch) yield this.pendingBatch;
	}

	private _get<T extends Codec<any>>(
		pointer: number,
		codec: T,
		includeBatch: boolean,
		readAheadSize?: number,
	): Codec.InferOutput<T> {
		const needed = codec.stride.size ?? readAheadSize ?? MAX_BLOCK_SIZE;
		const output = new Uint8Array(needed);

		let total = 0;
		let copied = 0;
		for (const region of this._regions(includeBatch)) {
			const regionSize = region === this.disk ? this.realizedDiskSize : region.size;

			if (copied >= output.length) break;
			const regionStart = total;
			total += regionSize;
			if (pointer >= total) continue; // region lies entirely before the pointer

			const localpointer = Math.max(0, pointer - regionStart);
			const available = regionSize - localpointer;
			const want = output.length - copied;
			const copy = Math.min(available, want);
			if (copy <= 0) continue;

			region.readInto(localpointer, copy, output.subarray(copied, copied + copy));
			copied += copy;
		}

		const [decoded] = codec.decode(output);
		return decoded;
	}

	get<T extends Codec<any>>(pointer: number, codec: T, options?: { readAheadSize?: number }): Codec.InferOutput<T> {
		return this._get(pointer, codec, false, options?.readAheadSize);
	}

	size() {
		return this.realizedDiskSize + this.staged.size + (this.frozen?.size ?? 0);
	}

	batch(): BlobStoreBatch {
		if (this.truncating) throw new Error("nah you can't do other shit like batching if you are truncating");
		if (this.pendingBatch) throw new Error("can't have concurrent batches man, can't calculate the size correctly");

		const region = this.pendingBatch ??= new MemoryRegion({ maxChunkSize: this.maxMemoryChunkSize });
		const prevsize = this.size();

		const size: BlobStoreBatch["size"] = () => {
			return prevsize + region.size;
		};

		const append: BlobStoreBatch["append"] = (data) => {
			const offset = size();
			region.append(data);
			return offset;
		};

		const get: BlobStoreBatch["get"] = (pointer, codec, options) => {
			return this._get(pointer, codec, true, options?.readAheadSize);
		};

		const apply: BlobStoreBatch["apply"] = () => {
			for (const chunk of region.chunks) {
				this.staged.append(chunk);
			}
			this.pendingBatch = null;
		};

		const discard: BlobStoreBatch["discard"] = () => {
			this.pendingBatch = null;
		};

		return { size, append, get, apply, discard };
	}

	// Synchronous snapshot — see Store.freeze. Splitting this out of flush() (where
	// it used to live) is what lets Atomic freeze every store at one instant so a
	// concurrent apply can't land in some stores' flush snapshot but not others'.
	// size() is unchanged across a freeze: the bytes simply move staged -> frozen.
	freeze(): void {
		if (this.frozen) return;
		if (this.truncating) throw new Error("can't freeze while truncating");
		this.frozen = this.staged;
		this.staged = new MemoryRegion({ maxChunkSize: this.maxMemoryChunkSize });
	}

	private pinning: boolean = false;
	pin(): void {
		if (this.pinning) throw new Error("already pinning");
		if (this.flushing) throw new Error("can't pin disk while flushing to it");
		if (this.truncating) throw new Error("can't pin disk while truncating it");
		this.pinning = true;
		try {
			// rollback.size is the pre-flush disk size. freeze() doesn't touch disk,
			// so recording disk.size here is correct whether pin runs before or after
			// freeze (Atomic freezes first, then pins).
			using rollback = Deno.openSync(this.rollbackPath, { create: true, write: true });
			writeFileSync(rollback, U64.encode(this.disk.size));
			rollback.syncSync();
		} finally {
			this.pinning = false;
		}
	}

	flush(): void {
		if (this.flushing) throw new Error("wtf are you doin man, you are already flushing");
		if (this.pinning) throw new Error("cant flush while pinning");
		if (this.truncating) throw new Error("can't flush while truncating");
		// Standalone callers don't freeze separately; Atomic does. Either way we
		// drain a stable frozen snapshot, never the live staged layer.
		if (!this.frozen) this.freeze();
		const frozen = this.frozen!;
		this.flushing = true;
		try {
			for (const chunk of frozen.chunks) {
				this.disk.append(chunk);
			}
			this.realizedDiskSize = this.disk.size;
		} finally {
			this.frozen = null;
			this.flushing = false;
		}
	}

	rollback(): void {
		// No pin recorded means nothing was ever flushed under WAL protection, so
		// there is nothing to undo. Atomic.recover() rolls back every store
		// uniformly, and pin() always fsyncs rollback.size before any disk mutation,
		// so a missing file can only mean "this store never got that far" — a no-op.
		// (Matches IndexStore.rollback, which already no-ops on a missing WAL.)
		if (!(existsSync(this.rollbackPath))) return;
		const [size] = U64.decode(Deno.readFileSync(this.rollbackPath));
		this.truncate(Number(size));
	}

	/**
	 * Delete the rollback size file. See {@link Store.finalize}.
	 */
	finalize(): void {
		try {
			Deno.removeSync(this.rollbackPath);
		} catch {
			/*  */
		}
	}

	truncate(size: number): void {
		if (this.truncating) throw new Error("A truncate is already in progress");
		if (this.pendingBatch) throw new Error("Can't truncate while a batch is open");
		if (this.staged.size > 0) throw new Error("Can't truncate while staged data is present; flush first");
		if (this.frozen) throw new Error("Can't truncate while a flush is pending/in progress");

		this.truncating = true;
		try {
			this.disk.truncate(size);
			this.realizedDiskSize = size;
		} finally {
			this.truncating = false;
		}
	}

	close(): void {
		this.disk.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
