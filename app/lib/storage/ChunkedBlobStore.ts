import { readFileFull, writeFileFull } from "../utils/fs.ts";
import { join } from "@std/path";

type CurrentChunk = {
	index: number;
	file: Deno.FsFile;
	size: number;
};

export class ChunkedBlobStore {
	private readonly chunkByteSize: number;
	private readonly directoryPath: string;

	private currentChunk: CurrentChunk | null = null;
	private initPromise: Promise<CurrentChunk> | null = null;
	private writeQueue: Promise<void> = Promise.resolve();

	constructor(directoryPath: string, chunkByteSize: number = 10 * 1024 * 1024 * 1024) {
		this.directoryPath = directoryPath;
		this.chunkByteSize = chunkByteSize;
	}

	private prepare(): Promise<CurrentChunk> {
		if (this.currentChunk) return Promise.resolve(this.currentChunk);
		if (this.initPromise) return this.initPromise;

		this.initPromise = (async () => {
			try {
				await Deno.mkdir(this.directoryPath, { recursive: true });

				const entries = Deno.readDir(this.directoryPath);
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
				const filePath = join(this.directoryPath, `chunk_${index}`);

				const file = await Deno.open(filePath, {
					create: true,
					write: true,
					append: true,
				});

				const { size } = await file.stat();
				await file.seek(size, Deno.SeekMode.Start);
				this.currentChunk = { index, file, size };
				return this.currentChunk;
			} catch (err) {
				this.initPromise = null; // Allow retry if initialization failed
				throw err;
			}
		})();

		return this.initPromise;
	}

	private async nextChunk(): Promise<CurrentChunk> {
		const index = await this.prepare().then((chunk) => chunk.index + 1);
		const filePath = join(this.directoryPath, `chunk_${index}`);
		const file = await Deno.open(filePath, {
			create: true,
			write: true,
			append: true,
		});
		this.currentChunk = { index, file, size: 0 };
		return this.currentChunk;
	}

	append(data: Uint8Array): Promise<number> {
		if (data.length > this.chunkByteSize) {
			throw new Error(`Data size (${data.length}) exceeds chunk limit (${this.chunkByteSize})`);
		}

		const operation = (async () => {
			await this.writeQueue;

			let current = await this.prepare();

			if (current.size + data.length > this.chunkByteSize) {
				current.file.close();
				current = await this.nextChunk();
			}

			const pointer = current.index * this.chunkByteSize + current.size;

			await writeFileFull(current.file, data);
			current.size += data.length;

			return pointer;
		})();

		// Synchronously update the queue tail
		this.writeQueue = operation.then(() => {}).catch(() => {});

		return operation;
	}

	async get(pointer: number, length: number): Promise<Uint8Array> {
		const chunkIndex = Math.floor(pointer / this.chunkByteSize);
		const chunkOffset = pointer % this.chunkByteSize;
		const filePath = join(this.directoryPath, `chunk_${chunkIndex}`);

		// Note: Using Deno.open per read is fine for low volume,
		// but consider a cache if you do thousands of reads/sec.
		const file = await Deno.open(filePath, { read: true });
		try {
			await file.seek(chunkOffset, Deno.SeekMode.Start);
			const buffer = new Uint8Array(length);
			const bytesRead = await readFileFull(file, buffer);

			if (bytesRead !== length) {
				throw new Error(`Underflow: requested ${length} bytes, but only read ${bytesRead}`);
			}
			return buffer;
		} finally {
			file.close();
		}
	}

	async close() {
		await this.writeQueue;
		if (this.currentChunk) {
			this.currentChunk.file.close();
			this.currentChunk = null;
			this.initPromise = null;
		}
	}
}
