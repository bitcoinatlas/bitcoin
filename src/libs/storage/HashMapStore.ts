import { Codec, StructCodec } from "@nomadshiba/codec";
import { join } from "@std/path";
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
	maxEntrySize: number;
	entryChunkSize: number;
	minBucketChunkSize: number;
};

const POINTER_SIZE = 8;

export class HashMapStore<Key extends Codec, Value extends Codec> extends Store implements Disposable {
	public readonly path: string;
	public readonly writable: boolean;

	private readonly key: Key;
	private readonly entry: StructCodec<{ key: Key; value: Value }>;
	private readonly maxRecordSize: number;

	private readonly entries: BlobStore;
	private readonly buckets: SharedSlotArray;
	private readonly loadFactor: LoadFactorOptions;

	private cursor: number;

	private constructor(options: HashMapStoreOptions<Key, Value>) {
		super();
		this.path = options.path;
		this.writable = options.writable;
		this.key = options.key;
		this.loadFactor = options.loadFactor;
		this.maxRecordSize = options.maxEntrySize + POINTER_SIZE;
		this.cursor = 0;
		this.entry = new StructCodec({ key: options.key, value: options.value });
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
		if (options.maxEntrySize <= 0) throw new Error("maxEntrySize must be positive");
		if (options.loadFactor.target <= 0 || options.loadFactor.maxDrift < 0) throw new Error("loadFactor must be positive");
		return new HashMapStore(options);
	}

	public mmap(): Uint8Array {
		return this.entries.mmap(this.cursor);
	}

	public commit(endOffset: number): void {
		if (!this.writable) throw new Error("commit on read-only store");
		this.cursor = endOffset;
	}

	public reveal(size: number): void {
		if (size < 0) throw new RangeError(`reveal ${size} must be non-negative`);
		this.cursor = size;
	}

	public persist(size: number): void {
		if (!this.writable) throw new Error("persist on read-only store");
		if (size < 0) throw new RangeError(`persist ${size} must be non-negative`);
	}

	public truncate(size: number): void {
		if (!this.writable) throw new Error("truncate on read-only store");
		if (size < 0) throw new RangeError(`truncate ${size} must be non-negative`);
	}

	public override size(): number {
		return this.cursor;
	}

	public next(): number {
		return this.entries.next(this.maxRecordSize + POINTER_SIZE, this.cursor);
	}

	public get(key: Codec.InferInput<Key>): Codec.InferOutput<Value> | undefined {
	}

	public getPointer(key: Codec.InferInput<Key>): number | undefined {
	}

	public getValueAndPointer(key: Codec.InferInput<Key>): [Codec.InferOutput<Value>, number] | undefined {
	}

	public has(key: Codec.InferInput<Key>): boolean {
		return this.get(key) !== undefined;
	}

	public override sync(): void {
		this.entries.sync();
	}

	public close(): void {
		this.entries.close();
	}

	public [Symbol.dispose](): void {
		this.close();
	}
}

function hashKey(key: Uint8Array): number {
	let hash = 0x811c9dc5;
	for (let index = 0; index < key.length; index++) hash = Math.imul(hash ^ key[index]!, 0x01000193);
	return hash >>> 0;
}
