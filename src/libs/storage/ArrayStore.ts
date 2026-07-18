import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";
import { ArrayCodec, type Codec, type FixedCodec } from "@nomadshiba/codec";
import { StoreAppendOnly } from "~/libs/storage/Store.ts";
import { BlobStore, CompressionOptions } from "./BlobStore.ts";

export type ArrayStoreOptions<T extends FixedCodec> = {
	path: string;
	rocksdb: RocksDatabase;
	codec: T;
	itemsPerChunk: number;
	writable: boolean;
	compression?: CompressionOptions;
};

export class ArrayStore<T extends FixedCodec> extends StoreAppendOnly implements Disposable {
	public readonly path: string;
	public get rocksdb() {
		return this.blob.rocksdb;
	}

	public readonly blob: BlobStore;
	public readonly codec: T;

	private constructor(blob: BlobStore, options: ArrayStoreOptions<T>) {
		super();
		this.blob = blob;
		this.codec = options.codec;
		this.path = options.path;
	}

	static open<T extends FixedCodec>(options: ArrayStoreOptions<T>): ArrayStore<T> {
		const blob = BlobStore.open({
			path: options.path,
			rocksdb: options.rocksdb,
			maxChunkSize: options.itemsPerChunk * options.codec.stride.size,
			writable: options.writable,
			compression: options.compression,
		});
		const self = new ArrayStore(blob, options);
		return self;
	}

	length(): number {
		return this.blob.size() / this.codec.stride.size;
	}

	get(index: number): Codec.InferOutput<T> | undefined {
		const length = this.length();
		if (index < 0) {
			throw new RangeError(`get out of bounds index=${index} length=${length}`);
		}
		if (index >= length) return undefined;
		return this.blob.get(index * this.codec.stride.size, this.codec);
	}

	slice(start: number, end: number): Codec.InferOutput<T>[] {
		const length = this.length();
		if (end > length) end = length;
		if (start < 0) {
			throw new RangeError(`slice out of bounds start=${start} end=${end} length=${length}`);
		}
		if (start > length) start = length;
		if (end <= start) return [];
		return this.blob.get(start * this.codec.stride.size, new ArrayCodec(this.codec, { size: end - start }));
	}

	push(item: Codec.InferInput<T>): number {
		const pointer = this.blob.append(this.codec.encode(item));
		return pointer / this.codec.stride.size;
	}

	pushMany(items: Codec.InferInput<T>[]): number {
		const stride = this.codec.stride.size;
		const buffer = new Uint8Array(items.length * stride);
		let offset = 0;
		for (const item of items) {
			this.codec.encodeInto(item, buffer, offset);
			offset += stride;
		}
		const pointer = this.blob.append(buffer);
		return pointer / stride;
	}

	pin(transaction?: Transaction): void {
		return this.blob.pin(transaction);
	}

	rollback(transaction?: Transaction): void {
		return this.blob.rollback(transaction);
	}

	truncate(length: number): void {
		return this.blob.truncate(length * this.codec.stride.size);
	}

	close(): void {
		this.blob.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
