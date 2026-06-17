import { Codec, U64 } from "@nomadshiba/codec";
import { join } from "@std/path";
import { MAX_BLOCK_SIZE } from "~/constants.ts";
import { Batch, Store } from "~/storage/Store.ts";
import { PromiseOrValue } from "~/types.ts";
import { readFileInto, writeFile } from "~/utils/fs.ts";

type Region = {
	size: number;
	append(bytes: Uint8Array): Promise<void> | void;
	readInto(offset: number, length: number, target: Uint8Array): PromiseOrValue<void>;
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

	_chunkPathCache: string[] = [];
	private _chunkPath(index: number) {
		return this._chunkPathCache[index] ??= join(this.path, `chunk_${index}`);
	}

	private constructor(options: DiskRegionOptions) {
		this.path = options.path;
		this.maxChunkSize = options.maxChunkSize;
		this.size = 0;
	}

	static async open(options: DiskRegionOptions): Promise<DiskRegion> {
		const self = new DiskRegion(options);
		await Deno.mkdir(self.path, { recursive: true });
		const files = Deno.readDir(self.path);

		let tailIndex = 0;
		const indexSet = new Set<number>([0]);
		for await (const file of files) {
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
				throw new Error("bro your chunks are fucked, has some    gaps   and  stuff");
			}
			const chunkStat = await Deno.stat(self._chunkPath(index));
			if (chunkStat.size !== self.maxChunkSize) {
				throw new Error(`chunk ${index} has a weird size size=${chunkStat.size}`);
			}
		}
		let tailChunkSize = 0;
		try {
			const tailChunkStat = await Deno.stat(self._chunkPath(tailIndex));
			if (tailChunkStat.size > self.maxChunkSize) {
				throw new Error(`your tail chunk is fat... size=${tailChunkStat.size}`);
			}
			tailChunkSize = tailChunkStat.size;
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
		}
		self._appender = {
			index: tailIndex,
			file: await Deno.open(self._chunkPath(tailIndex), { create: true, append: true }),
		};
		self.size = tailIndex * self.maxChunkSize + tailChunkSize;

		return self;
	}

	private _appender!: { file: Deno.FsFile; index: number };
	private _appending = false;
	async append(bytes: Uint8Array): Promise<void> {
		if (this._appending) throw new Error("you are trying to append back to back, check your logic");
		if (this._truncating) throw new Error("you are trying to append while truncating, check your logic");
		this._appending = true;
		try {
			let size = this.size;
			let appended = 0;
			while (appended < bytes.length) {
				const want = bytes.length - appended;
				const index = Math.floor(size / this.maxChunkSize);
				const taken = size % this.maxChunkSize;
				const available = this.maxChunkSize - taken;
				const append = Math.min(want, available);

				if (this._appender.index !== index) {
					const file = await Deno.open(this._chunkPath(index), { create: true, append: true });
					this._appender.file.close();
					this._appender.file = file;
					this._appender.index = index;
				}

				await writeFile(this._appender.file, bytes.subarray(appended, appended + append));
				await this._appender.file.sync();
				appended += append;
				size += append;
			}
			this.size = size;
		} finally {
			this._appending = false;
		}
	}

	async readInto(offset: number, length: number, target: Uint8Array): Promise<void> {
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

			using reader = await Deno.open(this._chunkPath(index), { read: true });
			await reader.seek(start, Deno.SeekMode.Start);
			await readFileInto(reader, target.subarray(copied, copied + read));

			copied += read;
			offset += read;
		}
	}

	private _truncating: boolean = false;
	async truncate(size: number) {
		if (this._appending) throw new Error("you cant truncate while appending");
		if (this._truncating) throw new Error("you are trying to truncate back to back, check your logic");
		this._truncating = true;

		const oldTail = this._appender.index;
		const newTail = Math.floor(size / this.maxChunkSize);

		this._appender.file.close();
		this._appender.index = -1;

		for (let index = newTail + 1; index <= oldTail; index++) {
			await Deno.remove(this._chunkPath(index));
		}

		const tailEnd = size % this.maxChunkSize;
		await Deno.truncate(this._chunkPath(newTail), tailEnd);

		this._appender.file = await Deno.open(this._chunkPath(newTail), { create: true, append: true });
		this._appender.index = newTail;

		this.size = size;

		// Should never fail to this point.
		// If it did data is corrupted
		// So that's why we are not doing try/catch
		this._truncating = false;
	}

	close() {
		this._appender.file.close();
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
	): Promise<Codec.InferOutput<T>>;
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

	private _disk!: DiskRegion;
	private _staged!: MemoryRegion;
	private _frozen: MemoryRegion | null | undefined;
	private _batch: MemoryRegion | null | undefined;
	private _realizedDiskSize: number;

	private _truncating = false;
	private _rollbackPath: string;

	private constructor(options: BlobStoreOptions) {
		super();
		this.path = options.path;
		this.maxDiskChunkSize = options.maxDiskChunkSize;
		this.maxMemoryChunkSize = options.maxMemoryChunkSize;

		this._realizedDiskSize = 0;
		this._rollbackPath = join(this.path, `rollback.size`);
	}

	static async open(options: BlobStoreOptions): Promise<BlobStore> {
		const { path } = options;
		const maxDiskChunkSize = options.maxDiskChunkSize;
		const maxMemoryChunkSize = options.maxMemoryChunkSize;

		const store = new BlobStore(options);
		store._disk = await DiskRegion.open({ path, maxChunkSize: maxDiskChunkSize });
		store._staged = new MemoryRegion({ maxChunkSize: maxMemoryChunkSize });
		store._realizedDiskSize = store._disk.size;

		return store;
	}

	private *_regions(includeBatch: boolean): Generator<Region> {
		yield this._disk;
		if (this._frozen) yield this._frozen;
		yield this._staged;
		if (includeBatch && this._batch) yield this._batch;
	}

	private async _get<T extends Codec<any>>(
		pointer: number,
		codec: T,
		includeBatch: boolean,
		readAheadSize?: number,
	): Promise<Codec.InferOutput<T>> {
		const output = new Uint8Array(codec.stride.size ?? readAheadSize ?? MAX_BLOCK_SIZE);

		let total = 0;
		let copied = 0;
		for (const region of this._regions(includeBatch)) {
			const regionSize = region === this._disk ? this._realizedDiskSize : region.size;

			if (copied >= output.length) break;
			const regionStart = total;
			total += regionSize;
			if (pointer >= total) continue; // region lies entirely before the pointer

			const localpointer = Math.max(0, pointer - regionStart);
			const available = regionSize - localpointer;
			const want = output.length - copied;
			const copy = Math.min(available, want);
			if (copy <= 0) continue;

			await region.readInto(localpointer, copy, output.subarray(copied, copied + copy));
			copied += copy;
		}

		const [decoded] = codec.decode(output);
		return decoded;
	}

	async get<T extends Codec<any>>(pointer: number, codec: T, options?: { readAheadSize?: number }): Promise<Codec.InferOutput<T>> {
		return this._get(pointer, codec, false, options?.readAheadSize);
	}

	size() {
		return this._realizedDiskSize + this._staged.size + (this._frozen?.size ?? 0);
	}

	batch(): BlobStoreBatch {
		if (this._truncating) throw new Error("nah you can't do other shit like batching if you are truncating");
		if (this._batch) throw new Error("can't have concurrent batches man, can't calculate the size correctly");

		const region = this._batch ??= new MemoryRegion({ maxChunkSize: this.maxMemoryChunkSize });
		const prevsize = this.size();

		const size: BlobStoreBatch["size"] = () => {
			return prevsize + region.size;
		};

		const append: BlobStoreBatch["append"] = (data) => {
			const offset = size();
			region.append(data);
			return offset;
		};

		const get: BlobStoreBatch["get"] = async (pointer, codec, options) => {
			return this._get(pointer, codec, true, options?.readAheadSize);
		};

		const apply: BlobStoreBatch["apply"] = () => {
			for (const chunk of region.chunks) {
				this._staged.append(chunk);
			}
			this._batch = null;
		};

		const discard: BlobStoreBatch["discard"] = () => {
			this._batch = null;
		};

		return { size, append, get, apply, discard };
	}

	private _pinning: boolean = false;
	async pin(): Promise<void> {
		if (this._pinning) throw new Error("already pinning");
		if (this._frozen) throw new Error("can't pin disk while flushing to it");
		if (this._truncating) throw new Error("can't pin disk while truncating it");
		this._pinning = true;
		try {
			using rollback = await Deno.open(this._rollbackPath, { create: true, write: true });
			await writeFile(rollback, U64.encode(this._disk.size));
			await rollback.sync();
		} finally {
			this._pinning = false;
		}
	}

	async flush(): Promise<void> {
		if (this._frozen) throw new Error("wtf are you doin man, you are already flushing");
		if (this._pinning) throw new Error("cant flush while pinning");
		if (this._truncating) throw new Error("can't flush while truncating");
		this._frozen = this._staged;
		this._staged = new MemoryRegion({ maxChunkSize: this.maxMemoryChunkSize });

		try {
			for (const chunk of this._frozen.chunks) {
				await this._disk.append(chunk);
			}
			this._realizedDiskSize = this._disk.size;
		} finally {
			this._frozen = null;
		}
	}

	async rollback(): Promise<void> {
		const [size] = U64.decode(await Deno.readFile(this._rollbackPath));
		await this.truncate(Number(size));
	}

	async truncate(size: number): Promise<void> {
		if (this._truncating) throw new Error("A truncate is already in progress");
		if (this._batch) throw new Error("Can't truncate while a batch is open");
		if (this._staged.size > 0) throw new Error("Can't truncate while staged data is present; flush first");
		if (this._frozen) throw new Error("Can't truncate while a flush is in progress");

		this._truncating = true;
		try {
			await this._disk.truncate(size);
			this._realizedDiskSize = size;
		} finally {
			this._truncating = false;
		}
	}

	close(): void {
		this._disk.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
