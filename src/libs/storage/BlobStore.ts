import { Codec, U64 } from "@nomadshiba/codec";
import { existsSync } from "@std/fs";
import { join } from "@std/path";
import { MAX_BLOCK_SIZE } from "~/constants.ts";
import { readFileIntoSync, writeFileSync } from "~/libs/fs/mod.ts";
import { Store } from "~/libs/storage/Store.ts";

type Region = {
	size: number;
	append(bytes: Uint8Array): void;
	readInto(offset: number, length: number, target: Uint8Array): void;
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
		this.disk.readInto(pointer, needed, output);
		const [decoded] = codec.decode(output);
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
		const size = existsSync(this.rollbackPath) ? Number(U64.decodeValue(Deno.readFileSync(this.rollbackPath))) : 0;
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

			const reader = this.readers[index] ??= Deno.openSync(this.chunkPath(index), { read: true });
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

		this.close();

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
