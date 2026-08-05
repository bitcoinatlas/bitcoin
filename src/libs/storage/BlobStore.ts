import { Codec } from "@nomadshiba/codec";
import { Advice, Mmap } from "@nomadshiba/mmap";
import { existsSync } from "@std/fs";
import { join } from "@std/path";
import { MiB, SECOND } from "~/constants.ts";
import { PARALLELISM_THREADS } from "~/env.ts";
import { Store } from "~/libs/storage/Store.ts";
import { ArchiveWorkerPool } from "~/libs/storage/ArchiveWorkerPool.ts";

import { createReadStream, createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import zlib from "node:zlib";

const { constants } = zlib;

const DEFAULT_PARALLELISM = Math.min(PARALLELISM_THREADS, Math.max(8, Math.floor(PARALLELISM_THREADS * .5)));
const RESTORE_STREAM_BUFFER_SIZE = 8 * MiB;
const RESTORE_SYNC_CHUNK_SIZE = 64 * MiB;

export type ArchiveOptions = {
	maxRestoredChunks: number;
	parallelism?: number;
	zstd: {
		archive: { [K in keyof typeof constants as K extends `ZSTD_c_${infer U}` ? U : never]?: number };
		restore: { [K in keyof typeof constants as K extends `ZSTD_d_${infer U}` ? U : never]?: number };
	};
};

export type BlobStoreOptions = {
	path: string;
	chunkSize: number;
};

type Chunk = { mapping: Mmap; bytes: Uint8Array };

export class BlobStore extends Store implements Disposable {
	public readonly path: string;
	public readonly chunkSize: number;
	private cursor: number;

	private pool: ArchiveWorkerPool | undefined;
	private maxRestoredChunks = 0;
	private archiveParams: zlib.ZstdOptions["params"] = {};
	private restoreSyncOptions: zlib.ZstdOptions = {};
	private restoreStreamOptions: zlib.ZstdOptions = {};
	private restoring = new Map<string, Promise<void>>();

	private chunks = new Map<number, Chunk>();
	private disposed = false;

	private constructor(options: BlobStoreOptions) {
		super();
		this.cursor = 0;
		this.path = options.path;
		this.chunkSize = options.chunkSize;
	}

	public static open(options: BlobStoreOptions): BlobStore {
		const self = new BlobStore(options);
		Deno.mkdirSync(self.path, { recursive: true });
		cleanUp(self.path);
		return self;
	}

	public sync(): void {
		for (const chunk of this.chunks.values()) chunk.mapping.flush();
	}

	public size(): number {
		return this.cursor;
	}

	public next(maxItemSize: number, from: number = this.size()): number {
		const room = this.chunkSize - (from % this.chunkSize);
		return room < maxItemSize ? from + room : from;
	}

	public reveal(size: number, _isBroadcast?: boolean): void {
		const current = this.size();
		if (size < current) throw new RangeError(`reveal size=${size} is behind the cursor (size=${current}); reveal only moves forward`);
		this.cursor = size;
	}

	/** append convenience: write at the cursor then reveal past it. sugar over commit+reveal. */
	public append(bytes: Uint8Array, from: number = this.next(bytes.length)): number {
		this.stage(from, bytes);
		this.reveal(from + bytes.length);
		return from;
	}

	public commit(size: number): void {
		return this.reveal(size);
	}

	public truncate(size: number): void {
		if (size < 0) throw new RangeError(`truncate size=${size} must be non-negative`);
		const current = this.size();
		const oldTailIndex = Math.floor(current / this.chunkSize);
		const newTailIndex = Math.floor(size / this.chunkSize);

		for (let index = oldTailIndex; index > newTailIndex; index--) {
			this.closeChunk(index);
			forget(chunkPath(this.path, index));
		}

		const tailPath = chunkPath(this.path, newTailIndex);
		if (this.pool && isArchived(tailPath)) {
			ensureRestored(tailPath, this.restoreSyncOptions);
			Deno.removeSync(archivePath(tailPath), { recursive: true });
			Deno.removeSync(archiveTmpPath(tailPath), { recursive: true });
		}

		this.cursor = size;
	}

	public get<T extends Codec>(pointer: number, codec: T): [Codec.InferOutput<T>, number] {
		const size = this.size();
		if (pointer >= size) throw new Error(`read at offset=${pointer} is past the cursor (size=${size})`);
		const index = Math.floor(pointer / this.chunkSize);
		const map = this.chunk(index);
		return codec.decode(map.bytes, pointer % this.chunkSize);
	}

	public async getAsync<T extends Codec>(pointer: number, codec: T): Promise<[Codec.InferOutput<T>, number]> {
		const size = this.size();
		if (pointer >= size) throw new Error(`read at offset=${pointer} is past the cursor (size=${size})`);
		const index = Math.floor(pointer / this.chunkSize);
		const map = await this.chunkAsync(index);
		return codec.decode(map.bytes, pointer % this.chunkSize);
	}

	// TODO: later get rid of this infavor of mmap()
	public stage(offset: number, bytes: Uint8Array): number {
		const size = this.size();
		if (offset < size) throw new Error(`write offset=${offset} is behind the cursor (size=${size}); writes never overwrite live data`);
		const index = Math.floor(offset / this.chunkSize);
		const start = offset % this.chunkSize;
		const available = this.chunkSize - start;
		if (bytes.length > available) {
			throw new Error(
				`write of ${bytes.length} bytes at offset=${offset} exceeds chunk ${index}'s remaining ${available} bytes; a write may never straddle a chunk boundary`,
			);
		}
		const map = this.chunk(index);
		map.bytes.set(bytes, start);
		return bytes.byteLength;
	}

	// `maxItemSize` is REQUIRED and is what makes this chunk-boundary safe: `begin`
	// defaults to `next(maxItemSize)`, which jumps to the next chunk if the current
	// one has less than `maxItemSize` bytes left. If a caller passes an explicit
	// `begin` (e.g. an offset it already computed elsewhere) that doesn't have room
	// for `maxItemSize`, this THROWS rather than silently handing back whatever's
	// left in the chunk — the old default-length behavior returned a truncated view
	// with no error, which let byte-indexed codec writes silently drop the tail of
	// a record whenever the cursor landed near a chunk boundary. That produced
	// permanent, deterministic corruption at that exact offset — never fixes
	// itself, which is why it kept reproducing at the same spot on every read.
	public mmap(maxItemSize: number, begin: number = this.next(maxItemSize)): Uint8Array {
		const index = Math.floor(begin / this.chunkSize);
		const start = begin % this.chunkSize;
		const available = this.chunkSize - start;
		if (maxItemSize > available) {
			throw new Error(
				`mmap of ${maxItemSize} bytes at offset=${begin} exceeds chunk ${index}'s remaining ${available} bytes; a write may never straddle a chunk boundary`,
			);
		}
		const map = this.chunk(index);
		return map.bytes.subarray(start, start + maxItemSize);
	}

	private chunk(index: number): Chunk {
		const cached = this.chunks.get(index);
		if (cached) return cached;

		const path = chunkPath(this.path, index);
		if (this.pool) {
			if (isArchived(path) && !existsSync(path)) this.tryMakeSpace(index);
			ensureRestored(path, this.restoreSyncOptions);
		}
		return this.map(index, Mmap.openSync(path, { write: true, ensureFileSize: this.chunkSize }));
	}

	public async chunkAsync(index: number): Promise<Chunk> {
		const cached = this.chunks.get(index);
		if (cached) return cached;

		const path = chunkPath(this.path, index);
		if (this.pool) {
			if (isArchived(path) && !existsSync(path)) this.tryMakeSpace(index);
			await ensureRestoredAsync(path, this.restoreStreamOptions, this.restoring);
		}
		return this.map(index, await Mmap.open(path, { write: true, ensureFileSize: this.chunkSize }));
	}

	private map(index: number, mapping: Mmap): Chunk {
		mapping.advise(Advice.Random);
		const chunk: Chunk = { mapping, bytes: mapping.bytes() };
		this.chunks.set(index, chunk);
		return chunk;
	}

	private tryMakeSpace(incoming: number): void {
		const restored: number[] = [];
		for (const index of this.chunks.keys()) {
			if (index === incoming) continue;
			const path = chunkPath(this.path, index);
			if (this.restoring.has(path) || !isArchived(path)) continue;
			restored.push(index);
		}
		let over = restored.length + 1 - this.maxRestoredChunks;
		for (const index of restored) {
			if (over <= 0) break;
			this.closeChunk(index);
			unrestore(chunkPath(this.path, index));
			over--;
		}
	}

	private closeChunk(index: number): void {
		const chunk = this.chunks.get(index);
		if (!chunk) return;
		chunk.mapping.close();
		this.chunks.delete(index);
	}

	public startArchiveWorkers(options: ArchiveOptions): void {
		if (this.pool) throw new Error("archiving already started on this store");
		if (options.maxRestoredChunks < 1) throw new Error("archive.maxRestoredChunks must be >= 1");

		this.maxRestoredChunks = options.maxRestoredChunks;
		this.pool = new ArchiveWorkerPool(options.parallelism ?? DEFAULT_PARALLELISM);
		this.archiveParams = mapZstdParams("ZSTD_c_", options.zstd.archive);
		const restoreParams = mapZstdParams("ZSTD_d_", options.zstd.restore);
		this.restoreSyncOptions = { chunkSize: RESTORE_SYNC_CHUNK_SIZE, params: restoreParams };
		this.restoreStreamOptions = { chunkSize: RESTORE_STREAM_BUFFER_SIZE, params: restoreParams };

		this.runArchiveLoop(this.pool).catch((e) => console.error("[archive] background loop died:", e));
	}

	private async runArchiveLoop(pool: ArchiveWorkerPool): Promise<void> {
		while (!this.disposed) {
			const tailIndex = Math.floor(this.size() / this.chunkSize);
			const batch: Promise<void>[] = [];
			for (let index = 0; index < tailIndex && !this.disposed; index++) {
				const path = chunkPath(this.path, index);
				if (isArchived(path) || !existsSync(path)) continue;
				const at = index;
				batch.push(
					archive(pool, at, path, this.archiveParams)
						.then(() => this.closeChunk(at))
						.catch((e) => console.error(`[archive] chunk ${at} failed:`, e)),
				);
			}
			if (batch.length) await Promise.all(batch);
			else await new Promise((resolve) => setTimeout(resolve, 10 * SECOND));
		}
	}

	public close(): void {
		this.disposed = true;
		this.pool?.dispose();
		this.pool = undefined;
		for (const chunk of this.chunks.values()) chunk.mapping.close();
		this.chunks.clear();
	}

	public [Symbol.dispose](): void {
		this.close();
	}
}

function chunkPath(root: string, index: number): string {
	return join(root, `chunk_${index}`);
}
function archivePath(path: string): string {
	return `${path}.zst`;
}
function archiveTmpPath(path: string): string {
	return `${path}.zst.tmp`;
}
function restoreTmpPath(path: string): string {
	return `${path}.raw.tmp`;
}
function lockPath(path: string): string {
	return `${path}.lock`;
}

function cleanUp(root: string): void {
	for (const entry of Deno.readDirSync(root)) {
		const path = join(root, entry.name);
		if (entry.isDirectory) {
			cleanUp(path);
			continue;
		}
		if (entry.isFile && entry.name.startsWith("chunk_") && entry.name.endsWith(".tmp")) Deno.removeSync(path);
	}
}

type ZLIB_CONSTANTS = typeof constants;
type ZLIB_ZSTD_PREFIX = "ZSTD_c_" | "ZSTD_d_";
type ZSTD_INFERED<P extends ZLIB_ZSTD_PREFIX = ZLIB_ZSTD_PREFIX> = {
	[K in keyof ZLIB_CONSTANTS as K extends `${P}${infer U}` ? Extract<U, string> : never]?: ZLIB_CONSTANTS[K];
};

function mapZstdParams<P extends ZLIB_ZSTD_PREFIX>(prefix: P, params: ZSTD_INFERED<P>): zlib.ZstdOptions["params"];
function mapZstdParams(prefix: string, params: { [K in string]?: ZSTD_INFERED[keyof ZSTD_INFERED] }): zlib.ZstdOptions["params"] {
	const out: zlib.ZstdOptions["params"] = {};
	for (const [key, value] of Object.entries(params)) {
		if (value === undefined) continue;
		out[constants[`${prefix}${key}` as never]] = value;
	}
	return out;
}

function isArchived(path: string): boolean {
	return existsSync(archivePath(path));
}

async function archive(pool: ArchiveWorkerPool, id: number, path: string, params: zlib.ZstdOptions["params"]): Promise<void> {
	if (existsSync(archivePath(path))) return;
	try {
		await Deno.stat(path);
	} catch (e) {
		if (e instanceof Deno.errors.NotFound) return;
		throw e;
	}

	const tmp = archiveTmpPath(path);
	await pool.archive(id, path, tmp, params);

	const lock = Deno.openSync(lockPath(path), { create: true, write: true, read: true });
	try {
		lock.lockSync(true);
		Deno.renameSync(tmp, archivePath(path));
		Deno.removeSync(path, { recursive: true });
	} finally {
		try {
			lock.unlockSync();
		} catch { /* */ }
		lock.close();
	}
}

function ensureRestored(path: string, options: zlib.ZstdOptions): void {
	if (existsSync(path)) return;
	if (!existsSync(archivePath(path))) return;

	const lock = Deno.openSync(lockPath(path), { create: true, write: true, read: true });
	try {
		lock.lockSync(true);
		if (existsSync(path)) return;

		const archived = Deno.readFileSync(archivePath(path));
		const raw = zlib.zstdDecompressSync(archived, options);
		const tmp = restoreTmpPath(path);
		Deno.writeFileSync(tmp, raw);
		Deno.renameSync(tmp, path);
	} finally {
		try {
			lock.unlockSync();
		} catch { /* */ }
		lock.close();
	}
}

function ensureRestoredAsync(path: string, options: zlib.ZstdOptions, restoring: Map<string, Promise<void>>): Promise<void> {
	if (existsSync(path)) return Promise.resolve();
	if (!existsSync(archivePath(path))) return Promise.resolve();

	const inFlight = restoring.get(path);
	if (inFlight) return inFlight;

	const promise = (async () => {
		const lock = Deno.openSync(lockPath(path), { create: true, write: true, read: true });
		try {
			await lock.lock(true);
			if (existsSync(path)) return;

			const tmp = restoreTmpPath(path);
			const source = createReadStream(archivePath(path), { highWaterMark: RESTORE_STREAM_BUFFER_SIZE });
			const transform = zlib.createZstdDecompress(options);
			const sink = createWriteStream(tmp, { highWaterMark: RESTORE_STREAM_BUFFER_SIZE });
			await pipeline(source, transform, sink);
			await Deno.rename(tmp, path);
		} finally {
			try {
				await lock.unlock();
			} catch { /* */ }
			lock.close();
		}
	})();
	restoring.set(path, promise);
	return promise.finally(() => restoring.delete(path));
}

function unrestore(path: string): void {
	if (!existsSync(archivePath(path))) return;
	const lock = Deno.openSync(lockPath(path), { create: true, write: true, read: true });
	try {
		lock.lockSync(true);
		Deno.removeSync(path, { recursive: true });
		Deno.removeSync(restoreTmpPath(path), { recursive: true });
	} finally {
		try {
			lock.unlockSync();
		} catch { /* */ }
		lock.close();
	}
}

function forget(path: string): void {
	Deno.removeSync(path, { recursive: true });
	Deno.removeSync(archivePath(path), { recursive: true });
	Deno.removeSync(archiveTmpPath(path), { recursive: true });
	Deno.removeSync(restoreTmpPath(path), { recursive: true });
	Deno.removeSync(lockPath(path), { recursive: true });
}
