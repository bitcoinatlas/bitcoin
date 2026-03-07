/**
 * CachedArrayStore - Fixed-size items kept in memory, synced to disk
 *
 * Like a resident array that persists. Not key-value, just indexed items.
 * Lazy preparation - data loaded on first use.
 */

import type { Codec } from "@nomadshiba/codec";
import { readFileFull, writeFileFull } from "../utils/fs.ts";

export class CachedArrayStore<T> {
	private filePath: string;
	private codec: Codec<T>;

	// Resident deserialized data
	private data: T[] = [];
	private dirty = new Set<number>();

	// Preparation state
	private prepared = false;
	private file: Deno.FsFile | null = null;

	constructor(filePath: string, codec: Codec<T>) {
		this.filePath = filePath;
		this.codec = codec;

		if (codec.stride < 0) {
			throw new Error("Codec must have fixed stride (>= 0)");
		}
	}

	/**
	 * Prepare the store by loading existing data from disk.
	 * Called automatically on first use, can be called manually.
	 */
	async prepare(): Promise<void> {
		if (this.prepared) return;

		this.file = await Deno.open(this.filePath, { create: true, read: true, write: true });

		const stat = await this.file.stat();
		if (stat.size > 0) {
			const count = Math.floor(stat.size / this.codec.stride);
			if (stat.size % this.codec.stride !== 0) {
				throw new Error(
					`Corrupt file: size ${stat.size} not divisible by stride ${this.codec.stride}`,
				);
			}

			const buffer = new Uint8Array(stat.size);
			await readFileFull(this.file, buffer);

			this.data = new Array(count);
			for (let i = 0; i < count; i++) {
				const bytes = buffer.subarray(i * this.codec.stride, (i + 1) * this.codec.stride);
				this.data[i] = this.codec.decode(bytes)[0];
			}
		}

		this.prepared = true;
	}

	async length(): Promise<number> {
		await this.prepare();
		return this.data.length;
	}

	async get(index: number): Promise<T | undefined> {
		await this.prepare();
		return this.data[index];
	}

	async getRange(start: number, count: number): Promise<T[]> {
		await this.prepare();
		return this.data.slice(start, start + count);
	}

	async push(item: T): Promise<number> {
		await this.prepare();
		const index = this.data.length;
		this.data.push(item);
		this.dirty.add(index);
		return index;
	}

	async pushMany(items: T[]): Promise<number[]> {
		await this.prepare();
		const startIndex = this.data.length;
		this.data.push(...items);
		for (let i = startIndex; i < this.data.length; i++) {
			this.dirty.add(i);
		}
		return items.map((_, i) => startIndex + i);
	}

	async truncate(newLength: number): Promise<void> {
		await this.prepare();
		if (newLength < 0 || newLength > this.data.length) {
			throw new Error(`Truncate newLength ${newLength} out of bounds`);
		}

		this.data.length = newLength;

		for (const dirtyIndex of this.dirty) {
			if (dirtyIndex >= newLength) {
				this.dirty.delete(dirtyIndex);
			}
		}

		if (this.file) {
			this.file.truncate(newLength * this.codec.stride);
		}
	}

	async flush(): Promise<void> {
		if (this.dirty.size === 0) return;
		if (!this.file) return;

		const sorted = Array.from(this.dirty).sort((a, b) => a - b);
		const ranges: Array<{ start: number; count: number }> = [];

		let currentStart = sorted[0]!;
		let currentCount = 1;

		for (let i = 1; i < sorted.length; i++) {
			if (sorted[i] === sorted[i - 1]! + 1) {
				currentCount++;
			} else {
				ranges.push({ start: currentStart, count: currentCount });
				currentStart = sorted[i]!;
				currentCount = 1;
			}
		}
		ranges.push({ start: currentStart, count: currentCount });

		for (const { start, count } of ranges) {
			const totalSize = count * this.codec.stride;
			const buffer = new Uint8Array(totalSize);

			for (let i = 0; i < count; i++) {
				const item = this.data[start + i]!;
				const encoded = this.codec.encode(item);
				if (encoded.length !== this.codec.stride) {
					throw new Error(`Encoded size ${encoded.length} != stride ${this.codec.stride}`);
				}
				buffer.set(encoded, i * this.codec.stride);
			}

			const offset = BigInt(start * this.codec.stride);
			await this.file.seek(offset, Deno.SeekMode.Start);
			await writeFileFull(this.file, buffer);
		}

		this.dirty.clear();
		await this.file.sync();
	}

	async close(): Promise<void> {
		await this.flush();
		if (this.file) {
			this.file.close();
			this.file = null;
		}
	}
}
