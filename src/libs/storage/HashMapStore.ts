import { Codec, StructCodec } from "@nomadshiba/codec";
import { equals } from "@std/bytes";
import { join } from "@std/path";
import { BlobStore, BlobStoreOptions } from "~/libs/storage/BlobStore.ts";
import { SharedSlotArray } from "~/libs/storage/SharedSlotArray.ts";
import { Store } from "~/libs/storage/Store.ts";

export type LoadFactorOptions = { target: number; maxDrift: number };
export type HashMapStoreOptions<Key extends Codec, Value extends Codec> = {
	path: string;
	key: Key;
	value: Value;
	maxEntrySize: number;
	loadFactor: LoadFactorOptions;
	writable: boolean;
	bucketCount?: number;
	blob: BlobStoreOptions;
};

const POINTER_SIZE = 8;

// TODO: made llm do this, didnt check it, check it, fix it if has behevoir that you dont like
export class HashMapStore<Key extends Codec, Value extends Codec> extends Store implements Disposable {
	public readonly path: string;
	public readonly writable: boolean;

	private readonly key: Key;
	private readonly keyValueCodec: StructCodec<{ key: Key; value: Value }>;
	private readonly maxRecordSize: number;

	private readonly entries: BlobStore;
	private readonly buckets: SharedSlotArray;
	private readonly targetLoadFactor: number;
	private readonly maxDrift: number;

	private bucketCount: number;
	private cursor = 0;
	private persistedSize = 0;
	private entryCount = 0;

	private constructor(options: HashMapStoreOptions<Key, Value>) {
		super();
		this.path = options.path;
		this.writable = options.writable;
		this.key = options.key;
		this.targetLoadFactor = options.loadFactor.target;
		this.maxDrift = options.loadFactor.maxDrift;
		this.maxRecordSize = options.maxEntrySize + POINTER_SIZE;
		this.keyValueCodec = new StructCodec({ key: options.key, value: options.value });
		this.bucketCount = Math.max(1, options.bucketCount ?? 1 << 12);
		this.entries = BlobStore.open(options.blob);
		this.buckets = SharedSlotArray.open({ path: join(options.path, "buckets"), writable: options.writable, minChunkSize: 1 << 20 });
	}

	static open<Key extends Codec, Value extends Codec>(
		options: HashMapStoreOptions<Key, Value>,
	): HashMapStore<Key, Value> {
		if (options.maxEntrySize <= 0) throw new Error("maxEntrySize must be positive");
		if (options.loadFactor.target <= 0 || options.loadFactor.maxDrift < 0) throw new Error("loadFactor must be positive");
		return new HashMapStore(options);
	}

	private hashKey(keyBytes: Uint8Array): number {
		let hash = 0x811c9dc5;
		for (let index = 0; index < keyBytes.length; index++) hash = Math.imul(hash ^ keyBytes[index]!, 0x01000193);
		return hash >>> 0;
	}

	private bucketIndexOf(keyBytes: Uint8Array): number {
		return this.hashKey(keyBytes) % this.bucketCount;
	}

	next(entrySize: number): number {
		return this.entries.next(entrySize + POINTER_SIZE, this.cursor);
	}

	mmap(): Uint8Array {
		return this.entries.mmap(this.cursor);
	}

	commit(endOffset: number): void {
		if (!this.writable) throw new Error("commit on read-only store");
		this.cursor = endOffset;
	}

	reveal(size: number): void {
		if (size < 0) throw new RangeError(`reveal ${size} must be non-negative`);
		this.cursor = size;
		this.persistedSize = size;
	}

	persist(newSize: number): void {
		if (!this.writable) throw new Error("persist on read-only store");
		this.buildIncremental(newSize);
		if (this.entryCount > 0) {
			const live = this.entryCount / this.bucketCount;
			if (live < this.targetLoadFactor * (1 - this.maxDrift) || live > this.targetLoadFactor * (1 + this.maxDrift)) {
				this.reshard(newSize);
			}
		}
	}

	truncate(size: number): void {
		if (!this.writable) throw new Error("truncate on read-only store");
		if (size < 0) throw new RangeError(`truncate ${size} must be non-negative`);
		this.fullRebuild(size);
	}

	/** Build buckets for the new area [persistedSize, size). Incremental, O(new entries). */
	private buildIncremental(size: number): void {
		let offset = this.entries.next(this.maxRecordSize, this.persistedSize);
		while (offset < size) {
			const view = this.entries.mmap(offset);
			const [record, consumed] = this.keyValueCodec.decode(view, POINTER_SIZE);
			const bucket = this.bucketIndexOf(this.key.encode(record.key));
			const head = Number(this.buckets.get(bucket));
			view.set(this.pointerToBytes(head), 0);
			this.buckets.set(bucket, BigInt(offset + 1));
			this.entryCount++;
			offset = this.entries.next(this.maxRecordSize, offset + POINTER_SIZE + consumed);
		}
		this.cursor = size;
		this.persistedSize = size;
	}

	/** Recompute bucketCount from the load factor and rebuild all heads under it. */
	private reshard(size: number): void {
		this.bucketCount = Math.max(1, Math.round(this.entryCount / this.targetLoadFactor));
		this.buildAllHeads(size);
	}

	/** Re-count entries, recompute bucketCount from the load factor, rebuild all heads. */
	private fullRebuild(size: number): void {
		let count = 0;
		let offset = this.entries.next(this.maxRecordSize, 0);
		while (offset < size) {
			const [, consumed] = this.keyValueCodec.decode(this.entries.mmap(offset), POINTER_SIZE);
			count++;
			offset = this.entries.next(this.maxRecordSize, offset + POINTER_SIZE + consumed);
		}
		this.entryCount = count;
		this.bucketCount = Math.max(1, Math.round(count / this.targetLoadFactor));
		this.cursor = size;
		this.persistedSize = size;
		this.buildAllHeads(size);
	}

	/** Zero all bucket slots, then walk entries oldest-first setting heads + previous. */
	private buildAllHeads(size: number): void {
		for (let index = 0; index < this.bucketCount; index++) this.buckets.set(index, 0n);
		let offset = this.entries.next(this.maxRecordSize, 0);
		while (offset < size) {
			const view = this.entries.mmap(offset);
			const [record, consumed] = this.keyValueCodec.decode(view, POINTER_SIZE);
			const bucket = this.bucketIndexOf(this.key.encode(record.key));
			const head = Number(this.buckets.get(bucket));
			view.set(this.pointerToBytes(head), 0);
			this.buckets.set(bucket, BigInt(offset + 1));
			offset = this.entries.next(this.maxRecordSize, offset + POINTER_SIZE + consumed);
		}
	}

	private findEntry(keyBytes: Uint8Array): { offset: number } | undefined {
		let head = Number(this.buckets.get(this.bucketIndexOf(keyBytes)));
		while (head !== 0) {
			const entryOffset = head - 1;
			if (entryOffset >= this.cursor) return undefined;
			const view = this.entries.mmap(entryOffset);
			const [record] = this.keyValueCodec.decode(view, POINTER_SIZE);
			if (equals(keyBytes, this.key.encode(record.key))) return { offset: entryOffset };
			head = Number(new DataView(view.buffer, view.byteOffset, POINTER_SIZE).getBigUint64(0, true));
		}
		return undefined;
	}

	get(key: Codec.InferInput<Key>): Codec.InferOutput<Value> | undefined {
		const found = this.findEntry(this.key.encode(key));
		if (!found) return undefined;
		return this.readValue(found.offset);
	}

	getPointer(key: Codec.InferInput<Key>): number | undefined {
		return this.findEntry(this.key.encode(key))?.offset;
	}

	getValueAndPointer(key: Codec.InferInput<Key>): [Codec.InferOutput<Value>, number] | undefined {
		const found = this.findEntry(this.key.encode(key));
		if (!found) return undefined;
		return [this.readValue(found.offset), found.offset];
	}

	private readValue(entryOffset: number): Codec.InferOutput<Value> {
		const view = this.entries.mmap(entryOffset);
		return this.keyValueCodec.decode(view, POINTER_SIZE)[0].value;
	}

	has(key: Codec.InferInput<Key>): boolean {
		return this.get(key) !== undefined;
	}

	private pointerToBytes(value: number): Uint8Array {
		const out = new Uint8Array(POINTER_SIZE);
		new DataView(out.buffer).setBigUint64(0, BigInt(value), true);
		return out;
	}

	override size(): number {
		return this.cursor;
	}

	override sync(): void {
		this.entries.sync();
	}

	close(): void {
		this.entries.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
