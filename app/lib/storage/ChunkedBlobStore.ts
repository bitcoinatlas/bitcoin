import { join } from "@std/path";
import { readFileFull, writeFileFull } from "../utils/fs.ts";
import { Mutex } from "../Mutex.ts";
import { exists } from "@std/fs";

type CurrentChunk = {
	index: number;
	path: string;
	size: number;
};

export type ChunkedBlobStoreOptions = {
	/** Maximum size of each chunk in bytes. Default is 1 GB. */
	chunkByteSize?: number;
};

export class ChunkedBlobStore {
	private readonly chunkByteSize: number;
	private readonly path: string;
	private readonly mutex = new Mutex();

	private currentChunk: CurrentChunk | null = null;

	constructor(directoryPath: string, options: ChunkedBlobStoreOptions = {}) {
		this.path = directoryPath;
		this.chunkByteSize = options.chunkByteSize ?? 1 * 1024 * 1024 * 1024;
	}

	private preparePromise: Promise<CurrentChunk> | null = null;
	private prepare(): Promise<CurrentChunk> {
		if (this.currentChunk) return Promise.resolve(this.currentChunk);
		return this.preparePromise ??= (async () => {
			try {
				await Deno.mkdir(this.path, { recursive: true });

				const entries = Deno.readDir(this.path);
				let maxIndex = -1;

				for await (const entry of entries) {
					if (entry.isFile && entry.name.startsWith("chunk_")) {
						const index = parseInt(entry.name.slice(6), 10);
						if (!isNaN(index) && index > maxIndex) {
							maxIndex = index;
						}
					}
				}

				const index = maxIndex === -1 ? 0 : maxIndex;
				const path = join(this.path, `chunk_${index}`);

				if (await exists(path, { isFile: true })) {
					const { size } = await Deno.stat(path);
					this.currentChunk = { index, path, size };
				} else {
					this.currentChunk = { index, path, size: 0 };
				}

				return this.currentChunk;
			} catch (err) {
				this.preparePromise = null; // Allow retry if initialization failed
				throw err;
			}
		})();
	}

	async append(data: Uint8Array): Promise<number> {
		if (data.length > this.chunkByteSize) {
			throw new Error(`Data size (${data.length}) exceeds chunk limit (${this.chunkByteSize})`);
		}

		const unlock = await this.mutex.lock();
		const current = await this.prepare();

		let file: Deno.FsFile | undefined;
		try {
			if (current.size + data.length > this.chunkByteSize) {
				const index = current.index + 1;
				const path = join(this.path, `chunk_${index}`);
				this.currentChunk = { index, path, size: 0 };
				file = await Deno.create(path);
			} else {
				file = await Deno.open(current.path, { write: true });
			}

			const pointer = current.index * this.chunkByteSize + current.size;
			await file.seek(0, Deno.SeekMode.End);
			await writeFileFull(file, data);
			current.size += data.length;
			return pointer; // starting point of the newly appened data
		} finally {
			file?.close();
			unlock();
		}
	}

	async get(pointer: number, length: number): Promise<Uint8Array> {
		const index = Math.floor(pointer / this.chunkByteSize);
		const offset = pointer % this.chunkByteSize;
		const path = join(this.path, `chunk_${index}`);
		const buffer = new Uint8Array(length);

		if (!await exists(path)) {
			return buffer;
		}

		// Not locking is fine, because we only append data at the end.
		using file = await Deno.open(path, { read: true });
		try {
			await file.seek(offset, Deno.SeekMode.Start);
			await readFileFull(file, buffer);
			return buffer;
		} finally {
			file.close();
		}
	}

	async truncate(pointerExclusive: number): Promise<void> {
		const targetIndex = Math.floor(pointerExclusive / this.chunkByteSize);
		const targetOffset = pointerExclusive % this.chunkByteSize;
		const current = await this.prepare();
		const unlock = await this.mutex.lock();
		try {
			for (let index = current.index; index > targetIndex; index--) {
				const path = join(this.path, `chunk_${index}`);
				await Deno.remove(path);
			}

			const path = join(this.path, `chunk_${targetIndex}`);
			await Deno.truncate(path, targetOffset);

			this.currentChunk = {
				index: targetIndex,
				path,
				size: targetOffset,
			};
		} finally {
			unlock();
		}
	}
}
