import { BytesCodec, type Codec, FixedCodec, StructCodec, VarInt } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import type { Batch, Store, WAL } from "~/lib/storage/Store.ts";
import { readFile, writeFile } from "~/lib/utils/fs.ts";

export interface ArrayStore<T extends FixedCodec> extends Store<ArrayStoreBatch<T>>, Disposable {
	get(index: number): Promise<Codec.InferOutput<T>>;
	slice(start: number, length: number): Promise<Codec.InferOutput<T>[]>;
	length(): number;
	truncate(newLength: number): Promise<void>;
	close(): void;
}

export interface ArrayStoreBatch<T extends FixedCodec> extends Batch {
	get(index: number): Promise<Codec.InferOutput<T>>;
	set(index: number, value: Codec.InferInput<T>): void;
	push(value: Codec.InferInput<T>): number;
	length(): number;
}

export type ArrayStoreOptions<T extends FixedCodec> = {
	name: string;
	path: string;
	codec: T;
	counter?: Codec<number>;
};

/**
 * WAL binary format:
 *
 *   [base_length: varint]                                   ← item count on disk when WAL was created
 *   [appends_count: varint][value bytes * appends_count]
 *   [sets_count: varint][{index: varint, value: bytes} * sets_count]
 *
 * On apply(): data.bin is first truncated back to base_length items, then appends and sets are
 * replayed from the WAL. This makes apply() idempotent: calling it multiple times (e.g. after
 * a crash mid-apply) always produces the same correct result.
 *
 * Appends have no index — they are packed raw bytes applied as a single bulk write.
 * Sets carry an explicit index and are applied after appends so index arithmetic is stable.
 */
export async function createArrayStore<T extends FixedCodec>(
	options: ArrayStoreOptions<T>,
): Promise<ArrayStore<T>> {
	const { name, path, codec } = options;
	const countCodec = options.counter ?? VarInt;

	const binPath = join(path, "data.bin");
	const walPath = join(path, `data.wal`);

	await Deno.mkdir(path, { recursive: true });
	const file = await Deno.open(binPath, { read: true, write: true, create: true });

	let diskLengthCache = (await file.stat()).size / codec.stride.size;
	if (!Number.isInteger(diskLengthCache)) {
		throw new Error("File size must be a multiple of codec stride");
	}

	// Mutex for seek+read/write operations on the shared file handle
	let ioLock: Promise<void> = Promise.resolve();
	function withLock<T>(fn: () => Promise<T>): Promise<T> {
		const next = ioLock.then(fn);
		ioLock = next.then(() => {}, () => {});
		return next;
	}

	// Staged appends: new entries in order (index = length + i)
	const stagedAppends: Uint8Array[] = [];
	// Staged sets: in-place updates to existing indices
	const stagedSets = new Map<number, Uint8Array>();

	function length(): number {
		return diskLengthCache + stagedAppends.length;
	}

	function close(): void {
		if (self.wal) {
			throw new Error("Can't perform this operation while a WAL is in progress");
		}
		file.close();
	}

	async function get(index: number): Promise<Codec.InferOutput<T>> {
		if (index < 0) {
			throw new Error("Index must be non-negative");
		}
		const totalLength = diskLengthCache + stagedAppends.length;
		if (index >= totalLength) {
			throw new Error("Index out of bounds");
		}
		// Check sets first
		if (stagedSets.has(index)) return codec.decode(stagedSets.get(index)!)[0];
		// Check staged appends
		if (index >= diskLengthCache) return codec.decode(stagedAppends[index - diskLengthCache]!)[0];
		const offset = index * codec.stride.size;
		return await withLock(async () => {
			await file.seek(offset, Deno.SeekMode.Start);
			const data = await readFile(file, codec.stride.size);
			const [value] = codec.decode(data);
			return value;
		});
	}

	async function slice(start: number, length: number): Promise<Codec.InferOutput<T>[]> {
		if (start < 0) throw new Error("start must be non-negative");
		if (length < 0) throw new Error("length must be non-negative");

		const totalLength = diskLengthCache + stagedAppends.length;
		const size = Math.min(length, totalLength - start);
		if (size <= 0) return [];

		const results = new Array<Codec.InferOutput<T>>(size);

		// How many entries of this slice come from disk vs staged
		const diskEnd = Math.min(start + size, diskLengthCache);
		const diskCount = Math.max(0, diskEnd - start);

		// Bulk read contiguous disk entries in one seek
		if (diskCount > 0) {
			const bulk = await withLock(async () => {
				await file.seek(start * codec.stride.size, Deno.SeekMode.Start);
				return await readFile(file, diskCount * codec.stride.size);
			});
			for (let i = 0; i < diskCount; i++) {
				const index = start + i;
				// stagedSets overrides disk
				if (stagedSets.has(index)) {
					results[i] = codec.decode(stagedSets.get(index)!)[0];
				} else {
					results[i] = codec.decode(bulk.subarray(i * codec.stride.size, (i + 1) * codec.stride.size))[0];
				}
			}
		}

		// Staged appends fill the remainder
		for (let i = diskCount; i < size; i++) {
			const index = start + i;
			// stagedSets can also override staged appends
			if (stagedSets.has(index)) {
				results[i] = codec.decode(stagedSets.get(index)!)[0];
			} else {
				results[i] = codec.decode(stagedAppends[index - diskLengthCache]!)[0];
			}
		}

		return results;
	}

	async function truncate(newLength: number): Promise<void> {
		if (batch) {
			throw new Error("Can't perform this operation while a batch is in progress");
		}
		if (self.wal) {
			throw new Error("Can't perform this operation while a WAL is in progress");
		}

		if (newLength < 0) throw new Error("newLength must be non-negative");
		const totalLength = diskLengthCache + stagedAppends.length;
		if (newLength > totalLength) {
			throw new Error(`newLength (${newLength}) exceeds current length (${totalLength})`);
		}

		// Drop staged appends past newLength
		const maxStagedAppends = Math.max(0, newLength - diskLengthCache);
		stagedAppends.length = maxStagedAppends;

		// Drop staged sets whose index no longer exists
		for (const index of stagedSets.keys()) {
			if (index >= newLength) stagedSets.delete(index);
		}

		// Truncate on-disk committed entries if newLength < length
		if (newLength < diskLengthCache) {
			await file.truncate(newLength * codec.stride.size);
			diskLengthCache = newLength;
		}
	}

	let batch: ArrayStoreBatch<T> | null = null;
	function batchFn(): ArrayStoreBatch<T> {
		if (batch) {
			throw new Error("Batch already in progress");
		}
		if (self.wal) {
			throw new Error("Can't start a batch while a WAL is in progress");
		}

		const batchSets = new Map<number, Uint8Array>();
		const batchAppends: Uint8Array[] = [];
		// snapshot of length at batch open (includes already-staged appends)
		const batchBaseLength = diskLengthCache + stagedAppends.length;

		batch = {
			async get(index: number) {
				if (index < 0) throw new Error("Index must be non-negative");
				if (index >= batchBaseLength + batchAppends.length) throw new Error("Index out of bounds");
				if (batchSets.has(index)) return codec.decode(batchSets.get(index)!)[0];
				if (index >= batchBaseLength) return codec.decode(batchAppends[index - batchBaseLength]!)[0];
				return await get(index);
			},
			set(index: number, value: Codec.InferInput<T>): void {
				if (index < 0) throw new Error("Index must be non-negative");
				if (index >= batchBaseLength + batchAppends.length) throw new Error("Index out of bounds");
				if (index >= batchBaseLength) {
					// Overwrite a within-batch append
					batchAppends[index - batchBaseLength] = codec.encode(value);
				} else {
					batchSets.set(index, codec.encode(value));
				}
			},
			push(value: Codec.InferInput<T>): number {
				const index = batchBaseLength + batchAppends.length;
				batchAppends.push(codec.encode(value));
				return index;
			},
			length(): number {
				return batchBaseLength + batchAppends.length;
			},
			apply(): void {
				for (const [index, value] of batchSets) {
					stagedSets.set(index, value);
				}
				for (const value of batchAppends) {
					stagedAppends.push(value);
				}
				// length stays as on-disk count; stagedAppends tracks the rest
				batchSets.clear();
				batchAppends.length = 0;
				batch = null;
			},
			discard(): void {
				batchSets.clear();
				batchAppends.length = 0;
				batch = null;
			},
		};

		return batch;
	}

	const setCodec = new StructCodec({ index: countCodec, value: new BytesCodec({ size: codec.stride.size }) });

	async function createWAL(): Promise<WAL> {
		if (self.wal) {
			throw new Error("WAL already exists");
		}
		if (batch) {
			throw new Error("Can't create a WAL while a batch is in progress");
		}

		// Section 0: base_length — item count on disk at WAL creation time
		const baseLength = diskLengthCache;
		const baseLengthBytes = countCodec.encode(baseLength);

		// Section 1: appends — count + packed raw bytes
		const appendCount = stagedAppends.length;
		const appendCountBytes = countCodec.encode(appendCount);
		const appendDataSize = appendCount * codec.stride.size;

		// Section 2: sets — count + [{index, value}...]
		const setEntries = Array.from(stagedSets.entries());
		const setCount = setEntries.length;
		const setCountBytes = countCodec.encode(setCount);
		const setData: Uint8Array[] = setEntries.map(([index, value]) => setCodec.encode({ index, value }));
		const setDataSize = setData.reduce((s, b) => s + b.length, 0);

		const buf = new Uint8Array(
			baseLengthBytes.length +
				appendCountBytes.length + appendDataSize +
				setCountBytes.length + setDataSize,
		);
		let pos = 0;

		buf.set(baseLengthBytes, pos);
		pos += baseLengthBytes.length;

		buf.set(appendCountBytes, pos);
		pos += appendCountBytes.length;
		for (const value of stagedAppends) {
			buf.set(value, pos);
			pos += codec.stride.size;
		}

		buf.set(setCountBytes, pos);
		pos += setCountBytes.length;
		for (const chunk of setData) {
			buf.set(chunk, pos);
			pos += chunk.length;
		}

		await Deno.writeFile(walPath, buf, { create: true });

		const wal = await getWAL();
		if (!wal) {
			throw new Error("Failed to create WAL");
		}
		self.wal = wal;
		return wal;
	}

	async function getWAL(): Promise<WAL | null> {
		if (!await exists(walPath)) {
			return null;
		}

		return {
			async apply(): Promise<void> {
				const buf = await Deno.readFile(walPath);
				let pos = 0;

				// Section 0: base_length — truncate disk back to this before replaying
				const [baseLength, baseLengthLen] = countCodec.decode(buf.subarray(pos));
				pos += baseLengthLen;
				await file.truncate(baseLength * codec.stride.size);
				diskLengthCache = baseLength;

				// Section 1: appends
				const [appendCount, appendCountLen] = countCodec.decode(buf.subarray(pos));
				pos += appendCountLen;

				if (appendCount > 0) {
					const appendBytes = buf.subarray(pos, pos + appendCount * codec.stride.size);
					await withLock(async () => {
						await file.seek(diskLengthCache * codec.stride.size, Deno.SeekMode.Start);
						await writeFile(file, appendBytes);
					});
					diskLengthCache += appendCount;
					stagedAppends.length = 0; // disk now consistent for appended range; clear before sets
					pos += appendCount * codec.stride.size;
				}

				// Section 2: sets
				const [setCount, setCountLen] = countCodec.decode(buf.subarray(pos));
				pos += setCountLen;

				for (let i = 0; i < setCount; i++) {
					const [entry, entryLen] = setCodec.decode(buf.subarray(pos));
					pos += entryLen;
					const offset = entry.index * codec.stride.size;
					await withLock(async () => {
						await file.seek(offset, Deno.SeekMode.Start);
						await writeFile(file, entry.value);
					});
					if (entry.index >= diskLengthCache) diskLengthCache = entry.index + 1;
				}
				stagedSets.clear();
			},
			async discard() {
				self.wal = null;
				stagedAppends.length = 0;
				stagedSets.clear();
				return await Deno.remove(walPath).catch(() => {/* ignore */});
			},
		};
	}

	const self: ArrayStore<T> = {
		name,
		get,
		slice,
		batch: batchFn,
		wal: await getWAL(),
		createWAL,
		truncate,
		length,
		close,
		[Symbol.dispose]() {
			self.close();
		},
	};

	return self;
}
