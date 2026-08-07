import { Codec, FixedCodec, StructCodec } from "@nomadshiba/codec";
import { IfNever } from "@project/utils";
import { equals } from "@std/bytes";
import { join } from "@std/path";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { SharedArrayStore } from "~/libs/storage/SharedArrayStore.ts";
import { Store } from "~/libs/storage/Store.ts";
import { NullableNumaricCodec } from "@project/codecs";

export type LoadFactorOptions = { target: number; maxDrift: number };
export type HashMapStoreOptions<Key extends Codec, Value extends Codec> = {
	commiter: boolean;
	path: string;

	loadFactor: LoadFactorOptions;
	entries: {
		key: Key;
		value: Value;
		chunkSize: number;
		pointer: FixedCodec<number>;
	} & IfNever<Extract<Key, FixedCodec> & Extract<Value, FixedCodec>, { maxEntrySize: number }, { maxEntrySize?: undefined }>;
	buckets: {
		initialSize: number;
		minChunkSize: number;
	};
	links: {
		index: FixedCodec<number>;
		minChunkSize: number;
	};
};

export class HashMapStore<Key extends Codec, Value extends Codec> extends Store implements Disposable {
	public readonly path: string;

	public readonly key: Key;
	public readonly value: Value;
	public readonly entry: StructCodec<{ key: Key; value: Value }>;

	private cursor: number;

	private maxEntrySize: number;
	private loadFactor: LoadFactorOptions;

	private buckets: SharedArrayStore<FixedCodec<number>>;
	private links: SharedArrayStore<StructCodec<{ prevIndex: NullableNumaricCodec<FixedCodec<number>>; entryPointer: FixedCodec<number> }>>;
	private entries: BlobStore;

	private commiter: boolean;
	private stagedBuckets: Map<number, number>;

	private lockFile: Deno.FsFile | null;

	private constructor(options: HashMapStoreOptions<Key, Value>) {
		super();
		this.path = options.path;
		this.key = options.entries.key;
		this.value = options.entries.value;
		this.entry = new StructCodec({ key: this.key, value: this.value });

		this.cursor = 0;

		this.maxEntrySize = options.entries.maxEntrySize ?? this.entry.stride.size!;
		this.loadFactor = options.loadFactor;
		this.commiter = options.commiter;
		this.stagedBuckets = new Map();

		this.lockFile = null;
		if (options.commiter) {
			const lockFile = Deno.openSync(join(this.path, "COMMITTER.lock"), { create: true, read: true, write: true });
			if (!lockFile.tryLockSync(true)) {
				lockFile.close();
				throw new Error(`another committer already holds ${join(this.path, "COMMITTER.lock")}`);
			}
			this.lockFile = lockFile;
		}

		this.buckets = SharedArrayStore.open({
			path: join(this.path, "buckets"),
			writable: options.commiter,
			item: options.links.index,
			minChunkSize: options.buckets.minChunkSize,
		});
		this.links = SharedArrayStore.open({
			path: join(this.path, "links"),
			writable: options.commiter,
			item: new StructCodec({ prevIndex: new NullableNumaricCodec(options.links.index), entryPointer: options.entries.pointer }),
			minChunkSize: options.links.minChunkSize,
		});
		this.entries = BlobStore.open({
			path: join(this.path, "entries"),
			chunkSize: options.entries.chunkSize,
		});

		if (options.commiter) {
			if (this.buckets.size() === 0) this.buckets.reveal(options.buckets.initialSize);
			if (this.links.size() === 0) {
				this.links.reveal(1);
				this.links.set(0, { prevIndex: null, entryPointer: 0 });
			}
		}
	}

	public static open<Key extends Codec, Value extends Codec>(options: HashMapStoreOptions<Key, Value>): HashMapStore<Key, Value> {
		return new HashMapStore(options);
	}

	public override size(): number {
		return this.cursor;
	}

	public put(key: Codec.InferInput<Key>, value: Codec.InferInput<Value>): number {
		const index = this.stage(key, value);
		this.reveal(this.cursor + 1);
		return index;
	}

	public stage(key: Codec.InferInput<Key>, value: Codec.InferInput<Value>): number {
		const index = this.cursor;
		const from = this.links.get(index).entryPointer;
		const written = this.entry.encodeInto({ key, value }, this.entries.mmap(this.maxEntrySize, from));
		this.entries.reveal(from + written);
		this.links.reveal(1);
		this.links.set(index + 1, { prevIndex: null, entryPointer: this.entries.next(this.maxEntrySize, from + written) });
		return index;
	}

	public get(key: Codec.InferInput<Key>): Codec.InferOutput<Value> | undefined {
		const keyBytes = this.key.encode(key);
		const bucket = hashKey(keyBytes) % this.buckets.size();
		let index = this.commiter && this.stagedBuckets.has(bucket) ? this.stagedBuckets.get(bucket)! : unbias(this.buckets.get(bucket));
		while (index !== null) {
			const link = this.links.get(index);
			const [entry] = this.entry.decode(this.entries.mmap(this.maxEntrySize, link.entryPointer));
			if (equals(this.key.encode(entry.key), keyBytes)) return entry.value;
			index = link.prevIndex;
		}
		return undefined;
	}

	public override reveal(size: number): void {
		if (size < this.cursor) {
			throw new RangeError(`reveal size=${size} is behind the cursor (size=${this.cursor}); reveal only moves forward`);
		}
		if (this.commiter) {
			for (let index = this.cursor; index < size; index++) {
				const link = this.links.get(index);
				const [entry] = this.entry.decode(this.entries.mmap(this.maxEntrySize, link.entryPointer));
				const bucket = hashKey(this.key.encode(entry.key)) % this.buckets.size();
				const head = this.stagedBuckets.has(bucket) ? this.stagedBuckets.get(bucket)! : unbias(this.buckets.get(bucket));
				this.links.set(index, { prevIndex: head, entryPointer: link.entryPointer });
				this.stagedBuckets.set(bucket, index);
			}
		}
		this.cursor = size;
	}

	public override truncate(size: number): void {
		if (size > this.cursor) throw new RangeError(`truncate size=${size} is ahead of the cursor (size=${this.cursor})`);
		for (let index = this.cursor - 1; index >= size; index--) {
			const link = this.links.get(index);
			const bucket = hashKey(this.key.encode(this.entry.decode(this.entries.mmap(this.maxEntrySize, link.entryPointer))[0].key)) %
				this.buckets.size();
			this.buckets.set(bucket, bias(link.prevIndex));
		}
		this.stagedBuckets.clear();
		const entriesEnd = this.links.get(size).entryPointer;
		this.links.truncate(size + 1);
		this.entries.truncate(entriesEnd);
		this.cursor = size;
	}

	public override sync(): void {
		this.entries.sync();
		this.links.sync();
		for (const [bucket, index] of this.stagedBuckets) this.buckets.set(bucket, bias(index));
		this.stagedBuckets.clear();
		this.buckets.sync();
	}

	public close(): void {
		this.entries.close();
		this.links.close();
		this.buckets.close();
		if (this.lockFile) {
			this.lockFile.unlockSync();
			this.lockFile.close();
			this.lockFile = null;
		}
	}

	public [Symbol.dispose](): void {
		this.close();
	}
}

function hashKey(key: Uint8Array): number {
	const len = key.length < 32 ? key.length : 32;
	const end = len & ~3;
	let h = 0;
	let i = 0;
	for (; i < end; i += 4) {
		const w = key[i]! | (key[i + 1]! << 8) | (key[i + 2]! << 16) | (key[i + 3]! << 24);
		h = (Math.imul(h, 31) + w) | 0;
	}
	for (; i < len; i++) {
		h = (Math.imul(h, 31) + key[i]!) | 0;
	}
	return h >>> 0;
}

function unbias(stored: number): number | null {
	return stored === 0 ? null : stored - 1;
}

function bias(index: number | null): number {
	return index === null ? 0 : index + 1;
}
