import type { Codec } from "@nomadshiba/codec";
import { readFileFull, writeFileFull } from "../utils/fs.ts";
import { Mutex } from "../Mutex.ts";

export class ArrayStore<T extends Codec<any>> {
	private readonly path: string;
	private readonly codec: T;
	private readonly mutex = new Mutex();
	private count = 0;

	constructor(path: string, codec: T) {
		this.path = path;
		this.codec = codec;
		if (codec.stride < 0) throw new Error("Codec must have fixed stride");
	}

	private preparePromise: Promise<void> | null = null;
	private prepare(): Promise<void> {
		if (this.preparePromise) return this.preparePromise;
		return this.preparePromise = (async () => {
			try {
				const file = await Deno.open(this.path, { create: true, read: true, write: true });
				const stat = await file.stat();
				file.close();
				if (stat.size % this.codec.stride !== 0) {
					throw new Error(`Corrupt file: size ${stat.size} not divisible by stride ${this.codec.stride}`);
				}
				this.count = stat.size / this.codec.stride;
			} catch (err) {
				this.preparePromise = null;
				throw err;
			}
		})();
	}

	async length(): Promise<number> {
		await this.prepare();
		return this.count;
	}

	async get(index: number): Promise<Codec.Infer<T> | undefined> {
		await this.prepare();
		if (index < 0 || index >= this.count) return undefined;

		const buffer = new Uint8Array(this.codec.stride);
		const file = await Deno.open(this.path, { read: true });
		try {
			await file.seek(BigInt(index * this.codec.stride), Deno.SeekMode.Start);
			await readFileFull(file, buffer);
			return this.codec.decode(buffer)[0];
		} finally {
			file.close();
		}
	}

	async range(start: number, count: number): Promise<Codec.Infer<T>[]> {
		await this.prepare();
		if (start < 0) start = 0;
		if (start >= this.count || count <= 0) return [];

		const actualCount = Math.min(count, this.count - start);
		const buffer = new Uint8Array(actualCount * this.codec.stride);
		const file = await Deno.open(this.path, { read: true });
		try {
			await file.seek(BigInt(start * this.codec.stride), Deno.SeekMode.Start);
			await readFileFull(file, buffer);
		} finally {
			file.close();
		}

		const result = new Array(actualCount);
		for (let i = 0; i < actualCount; i++) {
			const bytes = buffer.subarray(i * this.codec.stride, (i + 1) * this.codec.stride);
			result[i] = this.codec.decode(bytes)[0];
		}
		return result;
	}

	async push(item: Codec.Infer<T>): Promise<number> {
		const encoded = this.codec.encode(item);
		if (encoded.length !== this.codec.stride) {
			throw new Error(`Encoded size ${encoded.length} != stride ${this.codec.stride}`);
		}

		const unlock = await this.mutex.lock();
		try {
			await this.prepare();
			const file = await Deno.open(this.path, { append: true });
			try {
				await writeFileFull(file, encoded);
			} finally {
				file.close();
			}
			return ++this.count;
		} finally {
			unlock();
		}
	}

	async concat(items: Codec.Infer<T>[]): Promise<number[]> {
		if (items.length === 0) return [];

		const totalSize = items.length * this.codec.stride;
		const buffer = new Uint8Array(totalSize);
		for (let i = 0; i < items.length; i++) {
			const encoded = this.codec.encode(items[i]);
			if (encoded.length !== this.codec.stride) {
				throw new Error(`Encoded size ${encoded.length} != stride ${this.codec.stride}`);
			}
			buffer.set(encoded, i * this.codec.stride);
		}

		const unlock = await this.mutex.lock();
		try {
			await this.prepare();
			const startIndex = this.count;
			const file = await Deno.open(this.path, { append: true });
			try {
				await writeFileFull(file, buffer);
			} finally {
				file.close();
			}
			this.count += items.length;
			return items.map((_, i) => startIndex + i);
		} finally {
			unlock();
		}
	}

	async truncate(newLength: number): Promise<void> {
		await this.prepare();
		if (newLength < 0 || newLength > this.count) {
			throw new Error(`Truncate newLength ${newLength} out of bounds`);
		}
		await Deno.truncate(this.path, newLength * this.codec.stride);
		this.count = newLength;
	}
}
