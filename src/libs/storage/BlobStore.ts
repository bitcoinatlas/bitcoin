import { Codec, U64 } from "@nomadshiba/codec";
import { existsSync } from "@std/fs";
import { join } from "@std/path";
import { MAX_BLOCK_SIZE } from "~/constants.ts";
import { readFileIntoSync, writeFileSync } from "~/libs/fs/mod.ts";
import { Store } from "~/libs/storage/Store.ts";

type Region = {
	size: number;
	append(bytes: Uint8Array): void;
	readInto(offset: number, length: number, target: Uint8Array): number;
};

type DiskRegionOptions = {
	path: string;
	maxChunkSize: number;
};

export type BlobStoreOptions = {
	path: string;
	maxChunkSize: number;
};

export class BlobStore extends Store implements Disposable {
	public readonly path: string;
	private disk: DiskRegion;
	private rollbackPath: string;

	private constructor(disk: DiskRegion, options: BlobStoreOptions) {
		super();
		this.path = options.path;
		this.rollbackPath = join(this.path, `rollback.size`);
		this.disk = disk;
	}

	static open(options: BlobStoreOptions): BlobStore {
		const { path, maxChunkSize } = options;
		const store = new BlobStore(DiskRegion.open({ path, maxChunkSize }), options);
		return store;
	}

	get<T extends Codec<any>>(pointer: number, codec: T, options?: { readAheadSize?: number }): Codec.InferOutput<T> {
		const needed = codec.stride.size ?? options?.readAheadSize ?? MAX_BLOCK_SIZE;
		const output = new Uint8Array(needed);
		// FIX #2: readInto now clamps to what actually exists past `pointer` and
		// returns the number of bytes filled. For a fixed-stride codec `needed`
		// is exact so `filled === needed`. For a variable-stride readahead near
		// the tail, fewer bytes may exist; we decode only the valid prefix so we
		// never feed uninitialized buffer tail to the codec (no crash, no garbage).
		const filled = this.disk.readInto(pointer, needed, output);
		const [decoded] = codec.decode(filled === needed ? output : output.subarray(0, filled));
		return decoded;
	}

	append(data: Uint8Array) {
		const offset = this.size();
		this.disk.append(data);
		return offset;
	}

	size() {
		return this.disk.size;
	}

	private rollbackFile: Deno.FsFile | undefined;
	pin(): void {
		this.disk.sync();
		const rollback = this.rollbackFile ??= Deno.openSync(this.rollbackPath, { create: true, write: true });
		writeFileSync(rollback, U64.encode(this.disk.size));
		rollback.syncSync();
	}

	rollback(): void {
		let size: number;
		if (existsSync(this.rollbackPath)) {
			const [decoded] = U64.decode(Deno.readFileSync(this.rollbackPath));
			size = Number(decoded);
		} else {
			size = 0;
		}
		this.truncate(size);
	}

	truncate(size: number): void {
		this.disk.truncate(size);
	}

	close(): void {
		this.disk.close();
		this.rollbackFile?.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}

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
					this.appender.file.syncSync();
					const file = Deno.openSync(this.chunkPath(index), { create: true, append: true });
					this.appender.file.close();
					this.appender.file = file;
					this.appender.index = index;
				}

				writeFileSync(this.appender.file, bytes.subarray(appended, appended + append));
				appended += append;
				size += append;
			}
			this.size = size;
		} finally {
			this.appending = false;
		}
	}

	sync() {
		this.appender.file.syncSync();
	}

	private readers: Deno.FsFile[] = [];
	// FIX #2: clamp the requested length to bytes that actually exist, and
	// return how many were filled so the caller can decode only the valid range.
	readInto(offset: number, length: number, target: Uint8Array): number {
		if (offset >= this.size) {
			throw new Error(`yeah you wanna read from offset=${offset}, but all i have is size=${this.size}`);
		}

		// Never try to read past the end of written data.
		const clamped = Math.min(length, this.size - offset);

		let copied = 0;
		while (copied < clamped) {
			const want = clamped - copied;
			const index = Math.floor(offset / this.maxChunkSize);
			const start = offset % this.maxChunkSize;
			const available = this.maxChunkSize - start;
			const read = Math.min(want, available);

			const reader = this.readers[index] ??= Deno.openSync(this.chunkPath(index), { read: true });
			reader.seekSync(start, Deno.SeekMode.Start);
			readFileIntoSync(reader, target.subarray(copied, copied + read));

			copied += read;
			offset += read;
		}
		return copied;
	}

	private truncating: boolean = false;
	truncate(size: number): void {
		if (this.appending) throw new Error("you cant truncate while appending");
		if (this.truncating) throw new Error("you are trying to truncate back to back, check your logic");
		if (size > this.size) throw new Error(`cant truncate upward size=${size} current=${this.size}`);
		this.truncating = true;

		const oldTail = this.appender.index;
		const newTail = Math.floor(size / this.maxChunkSize);
		const tailEnd = size % this.maxChunkSize;

		this.close();

		// FIX #5: delete high-to-low. If a crash happens mid-truncate, the
		// surviving chunks are always a contiguous prefix [0..k], never a set
		// with a gap. A gap would make DiskRegion.open throw ("chunks are
		// fucked") and brick recovery. High-to-low guarantees no gap.
		for (let index = oldTail; index > newTail; index--) {
			// Tolerate already-removed chunks so a re-run of a crashed rollback
			// is idempotent.
			try {
				Deno.removeSync(this.chunkPath(index));
			} catch (e) {
				if (!(e instanceof Deno.errors.NotFound)) throw e;
			}
		}

		// FIX #1: when size lands exactly on a chunk boundary (tailEnd === 0),
		// chunk `newTail` may not exist yet (it is only created when data first
		// spills into it). Create-or-truncate it to length 0 explicitly instead
		// of calling Deno.truncateSync on a possibly-missing file.
		if (tailEnd === 0) {
			// Ensure newTail exists and is empty.
			using _ = Deno.openSync(this.chunkPath(newTail), { create: true, write: true, truncate: true });
		} else {
			Deno.truncateSync(this.chunkPath(newTail), tailEnd);
		}

		this.appender.file = Deno.openSync(this.chunkPath(newTail), { create: true, append: true });
		this.appender.index = newTail;

		this.size = size;

		// Should never fail to this point.
		// If it did data is corrupted
		// So that's why we are not doing try/catch
		this.truncating = false;
	}

	close() {
		for (const reader of this.readers) {
			if (!reader) continue;
			reader.close();
		}
		this.readers.length = 0;
		this.appender.file.close();
		this.appender.index = -1;
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
