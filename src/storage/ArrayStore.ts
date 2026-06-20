import { ArrayCodec, type Codec, type FixedCodec } from "@nomadshiba/codec";
import { Batch, Store } from "~/storage/Store.ts";
import { BlobStore, type BlobStoreBatch } from "./BlobStore.ts";

export interface ArrayStoreBatch<T> extends Batch {
	push(item: T): number;
	get(index: number): Promise<T | undefined>;
	length(): number;
}

export type ArrayStoreOptions<T extends FixedCodec<any>> = {
	path: string;
	codec: T;
	memoryItemsPerChunk: number;
	diskItemsPerChunk: number;
};

export class ArrayStore<T extends FixedCodec<any>> extends Store<ArrayStoreBatch<Codec.InferOutput<T>>> implements Disposable {
	public readonly path: string;

	private readonly blob: BlobStore;
	private readonly codec: T;

	private constructor(blob: BlobStore, options: ArrayStoreOptions<T>) {
		super();
		this.blob = blob;
		this.codec = options.codec;
		this.path = options.path;
	}

	static async open<T extends FixedCodec<any>>(options: ArrayStoreOptions<T>): Promise<ArrayStore<T>> {
		const blob = await BlobStore.open({
			path: options.path,
			maxMemoryChunkSize: options.memoryItemsPerChunk * options.codec.stride.size,
			maxDiskChunkSize: options.diskItemsPerChunk * options.codec.stride.size,
		});
		const self = new ArrayStore(blob, options);
		return self;
	}

	private get stride(): number {
		return this.codec.stride.size;
	}

	length(): number {
		return this.blob.size() / this.stride;
	}

	async get(index: number): Promise<Codec.InferOutput<T> | undefined> {
		const length = this.length();
		if (index < 0) {
			throw new RangeError(`get out of bounds index=${index} length=${length}`);
		}
		if (index >= length) return undefined;
		return this.blob.get(index * this.stride, this.codec);
	}

	async slice(start: number, end: number): Promise<Codec.InferOutput<T>[]> {
		const length = this.length();
		if (end > length) end = length;
		if (start < 0) {
			throw new RangeError(`slice out of bounds start=${start} end=${end} length=${length}`);
		}
		// start/end are item indices, so the blob pointer must be a byte offset.
		// Previously `start` was passed straight through as a byte pointer, reading
		// from byte `start` instead of `start * stride` for any start > 0.
		return this.blob.get(start * this.stride, new ArrayCodec(this.codec, { size: end - start }));
	}

	batch(): ArrayStoreBatch<Codec.InferOutput<T>> {
		const blob: BlobStoreBatch = this.blob.batch();
		const stride = this.stride;
		const codec = this.codec;

		const length: ArrayStoreBatch<Codec.InferOutput<T>>["length"] = () => {
			return blob.size() / stride;
		};

		const push: ArrayStoreBatch<Codec.InferOutput<T>>["push"] = (item) => {
			const pointer = blob.append(codec.encode(item));
			return pointer / stride;
		};

		const get: ArrayStoreBatch<Codec.InferOutput<T>>["get"] = (index) => {
			return blob.get(index * stride, codec);
		};

		const apply: ArrayStoreBatch<Codec.InferOutput<T>>["apply"] = () => blob.apply();
		const discard: ArrayStoreBatch<Codec.InferOutput<T>>["discard"] = () => blob.discard();

		return { length, push, get, apply, discard };
	}

	freeze(): void {
		this.blob.freeze();
	}

	async pin(): Promise<void> {
		return this.blob.pin();
	}

	async flush(): Promise<void> {
		return this.blob.flush();
	}

	async rollback(): Promise<void> {
		return this.blob.rollback();
	}

	async finalize(): Promise<void> {
		return this.blob.finalize();
	}

	async truncate(length: number): Promise<void> {
		return this.blob.truncate(length * this.stride);
	}

	close(): void {
		this.blob.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
