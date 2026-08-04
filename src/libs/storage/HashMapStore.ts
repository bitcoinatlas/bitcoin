import { Codec, StructCodec, U64 } from "@nomadshiba/codec";
import { equals } from "@std/bytes";
import { join } from "@std/path";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { SharedSlotArray } from "~/libs/storage/SharedSlotArray.ts";
import { Store } from "~/libs/storage/Store.ts";

export type LoadFactorOptions = { target: number; maxDrift: number };
export type HashMapStoreOptions<Key extends Codec, Value extends Codec> = {
	writable: boolean;
	path: string;
	key: Key;
	value: Value;
	loadFactor: LoadFactorOptions;
	entryChunkSize: number;
	minBucketChunkSize: number;
	maxEntrySize: number;
};

const POINTER_SIZE = SharedSlotArray.BYTES_PER_SLOT;

export class HashMapStore<Key extends Codec, Value extends Codec> extends Store implements Disposable {
	public readonly path: string;
	public readonly writable: boolean;

	public readonly key: Key;
	public readonly value: Value;
	public readonly entry: StructCodec<{ key: Key; value: Value }>;
	private readonly maxEntrySize: number;

	private readonly entries: BlobStore;
	private readonly buckets: SharedSlotArray;
	private readonly loadFactor: LoadFactorOptions;

	private cursor: number;

	/**
	 * Per-worker in-memory view of entries revealed locally but not yet persisted
	 * into the shared buckets. Populated by a manual `reveal` (isBroadcast=false)
	 * so this worker can find its own writes via get/has before pin(). Wired into
	 * the real buckets and cleared by `persist`; dropped by `truncate`. Pure
	 * cache: safe to rebuild by re-walking entries.
	 */
	private readonly stage = new FastUint8ArrayMap<number>();

	/** scratch for encoding a lookup key without allocating per read. */
	private readonly keyScratch_: Uint8Array;

	private constructor(options: HashMapStoreOptions<Key, Value>) {
		super();
		this.path = options.path;
		this.writable = options.writable;
		this.key = options.key;
		this.value = options.value;
		this.entry = new StructCodec({ key: options.key, value: options.value });
		this.maxEntrySize = options.maxEntrySize;
		this.keyScratch_ = new Uint8Array(options.maxEntrySize);
		this.loadFactor = options.loadFactor;
		this.cursor = 0;
		this.entries = BlobStore.open({
			path: join(options.path, "entries"),
			chunkSize: options.entryChunkSize,
		});
		this.buckets = SharedSlotArray.open({
			path: join(options.path, "buckets"),
			writable: options.writable,
			minChunkSize: options.minBucketChunkSize,
		});
	}

	public static open<Key extends Codec, Value extends Codec>(
		options: HashMapStoreOptions<Key, Value>,
	): HashMapStore<Key, Value> {
		if (options.loadFactor.target <= 0 || options.loadFactor.maxDrift < 0) throw new Error("loadFactor must be positive");
		return new HashMapStore(options);
	}

	public size(): number {
		return this.cursor;
	}

	public next(from?: number): number {
		return this.entries.next(POINTER_SIZE + this.maxEntrySize, from);
	}

	public reveal(size: number, isBroadcast?: boolean): void {
		if (size < 0) throw new RangeError(`reveal ${size} must be non-negative`);
		const current = this.cursor;
		if (size === current) return;
		if (size < current) throw new RangeError(`reveal ${size} is behind the cursor (size=${current}); reveal only moves forward`);

		// Broadcast reveals cover regions already persisted into the shared
		// buckets, so they're findable there — no per-worker staging needed. A
		// manual reveal exposes this worker's own not-yet-persisted entries, so
		// stage them (key -> entryOffset) so get/has can see them before pin().
		if (!isBroadcast) {
			for (let entryOffset = this.next(current); entryOffset < size;) {
				const recordOffset = entryOffset - POINTER_SIZE;
				const recordBytes = this.mmap(recordOffset);
				const keyOffset = POINTER_SIZE;
				const [, keySize] = this.key.decode(recordBytes, keyOffset);
				// Copy the key: the map holds keys by reference, but recordBytes is
				// a live view into the moving mmap and must not be retained.
				const keyBytes = recordBytes.slice(keyOffset, keyOffset + keySize);
				this.stage.set(keyBytes, entryOffset);
				const valueOffset = keyOffset + keySize;
				const [, valueSize] = this.value.decode(recordBytes, valueOffset);
				entryOffset = this.next(entryOffset + keySize + valueSize);
			}
		}

		this.cursor = size;
	}

	public mmap(begin?: number, length?: number): Uint8Array {
		return this.entries.mmap(begin, length);
	}

	public commit(key: Codec.InferInput<Key>, value: Codec.InferInput<Value>, offset?: number): number {
		offset ??= this.next(this.size());
		return this.entry.encodeInto({ key, value }, this.mmap(offset));
	}

	private pointerView_: BigUint64Array = new BigUint64Array(1);
	private pointerViewBytes_: Uint8Array = new Uint8Array(this.pointerView_.buffer);
	public persist(size: number): void {
		if (!this.writable) throw new Error("persist on read-only store");
		if (size === this.cursor) return;
		if (size < this.cursor) throw new RangeError(`persist ${size} can't be smaller than current size`);
		const current = this.cursor;
		for (let entryOffset = this.next(current); entryOffset < size;) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			const bucket = hashKey(recordBytes, keyOffset, keyOffset + keySize) % this.buckets.size();
			const prevBucketValue = this.buckets.get(bucket);
			this.pointerView_[0] = prevBucketValue;
			recordBytes.set(this.pointerViewBytes_, 0);
			this.buckets.set(bucket, BigInt(entryOffset));
			const valueOffset = keyOffset + keySize;
			const [, valueSize] = this.value.decode(recordBytes, valueOffset);
			const entrySize = keySize + valueSize;
			entryOffset = this.next(entryOffset + entrySize);
		}
		this.cursor = size;
		// Entries up to `size` are now wired into the shared buckets, so the stage
		// (which only exists to expose not-yet-persisted entries) is redundant.
		this.stage.clear();
		// TODO: Load factor check and update
	}

	public truncate(size: number): void {
		if (!this.writable) throw new Error("truncate on read-only store");
		if (size === this.cursor) return;
		if (size < 0) throw new RangeError(`truncate ${size} must be non-negative`);
		if (size > this.cursor) throw new RangeError(`truncate ${size} can't be bigger than current size`);
		const current = this.cursor;
		for (let entryOffset = this.next(size); entryOffset < current;) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const [candidateBucketValue] = U64.decode(recordBytes);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			const bucket = hashKey(recordBytes, keyOffset, keyOffset + keySize) % this.buckets.size();
			const currentBucketValue = this.buckets.get(bucket);
			if (candidateBucketValue < currentBucketValue) {
				this.buckets.set(bucket, candidateBucketValue);
			}
			const valueOffset = keyOffset + keySize;
			const [, valueSize] = this.value.decode(recordBytes, valueOffset);
			const entrySize = keySize + valueSize;
			entryOffset = this.next(entryOffset + entrySize);
		}
		this.cursor = size;
		// Drop the staged (un-persisted) view; it's a pure cache rebuilt on the
		// next manual reveal.
		this.stage.clear();
		// TODO: Load factor check and update
	}

	/** decode the value stored at an entry offset (key -> value record layout). */
	private valueAt(entryOffset: number): Codec.InferOutput<Value> {
		const recordBytes = this.mmap(entryOffset - POINTER_SIZE);
		const keyOffset = POINTER_SIZE;
		const [, keySize] = this.key.decode(recordBytes, keyOffset);
		const [value] = this.value.decode(recordBytes, keyOffset + keySize);
		return value;
	}

	/**
	 * Encode a lookup key into the shared scratch buffer, returning the used view.
	 * Safe for reads (hashKey/equals/stage.get) that don't retain the bytes; do
	 * NOT use for anything that stores the key by reference (e.g. stage.set).
	 */
	private encodeKey(key: Codec.InferInput<Key>): Uint8Array {
		const size = this.key.encodeInto(key, this.keyScratch_, 0);
		return this.keyScratch_.subarray(0, size);
	}

	/** decode the entry stored at an entry offset, returning [key, value]. */
	public getEntry(entryOffset: number): [Codec.InferOutput<Key>, Codec.InferOutput<Value>] {
		const recordBytes = this.mmap(entryOffset - POINTER_SIZE);
		const keyOffset = POINTER_SIZE;
		const [key, keySize] = this.key.decode(recordBytes, keyOffset);
		const [value] = this.value.decode(recordBytes, keyOffset + keySize);
		return [key, value];
	}

	public get(key: Codec.InferInput<Key>): Codec.InferOutput<Value> | undefined {
		const encoded = this.encodeKey(key);
		const staged = this.stage.get(encoded);
		if (staged !== undefined) {
			const [, value] = this.getEntry(staged);
			return value;
		}
		const bucket = hashKey(encoded) % this.buckets.size();
		let entryOffset = Number(this.buckets.get(bucket));
		while (entryOffset !== 0) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			if (equals(encoded, recordBytes.subarray(keyOffset, keyOffset + keySize))) {
				const [value] = this.value.decode(recordBytes, keyOffset + keySize);
				return value;
			}
			const [next] = U64.decode(recordBytes);
			entryOffset = Number(next);
		}
		return undefined;
	}

	public getPointer(key: Codec.InferInput<Key>): number | undefined {
		const encoded = this.encodeKey(key);
		const staged = this.stage.get(encoded);
		if (staged !== undefined) return staged;
		const bucket = hashKey(encoded) % this.buckets.size();
		let entryOffset = Number(this.buckets.get(bucket));
		while (entryOffset !== 0) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			if (equals(encoded, recordBytes.subarray(keyOffset, keyOffset + keySize))) return entryOffset;
			const [next] = U64.decode(recordBytes);
			entryOffset = Number(next);
		}
		return undefined;
	}

	public getValueAndPointer(key: Codec.InferInput<Key>): [Codec.InferOutput<Value>, number] | undefined {
		const encoded = this.encodeKey(key);
		const staged = this.stage.get(encoded);
		if (staged !== undefined) {
			const [, value] = this.getEntry(staged);
			return [value, staged];
		}
		const bucket = hashKey(encoded) % this.buckets.size();
		let entryOffset = Number(this.buckets.get(bucket));
		while (entryOffset !== 0) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			if (equals(encoded, recordBytes.subarray(keyOffset, keyOffset + keySize))) {
				const [value] = this.value.decode(recordBytes, keyOffset + keySize);
				return [value, entryOffset];
			}
			const [next] = U64.decode(recordBytes);
			entryOffset = Number(next);
		}
		return undefined;
	}

	public has(key: Codec.InferInput<Key>): boolean {
		const encoded = this.encodeKey(key);
		if (this.stage.has(encoded)) return true;
		const bucket = hashKey(encoded) % this.buckets.size();
		let entryOffset = Number(this.buckets.get(bucket));
		while (entryOffset !== 0) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			if (equals(encoded, recordBytes.subarray(keyOffset, keyOffset + keySize))) return true;
			const [next] = U64.decode(recordBytes);
			entryOffset = Number(next);
		}
		return false;
	}

	public sync(): void {
		this.entries.sync();
	}

	public close(): void {
		this.entries.close();
	}

	public [Symbol.dispose](): void {
		this.close();
	}
}

function hashKey(source: Uint8Array, begin: number = 0, end: number = source.length): number {
	let hash = 0x811c9dc5;
	for (let index = begin; index < end; index++) hash = Math.imul(hash ^ source[index]!, 0x01000193);
	return hash >>> 0;
}
