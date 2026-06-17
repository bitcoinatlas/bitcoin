import { ArrayCodec, type Codec, type FixedCodec } from "@nomadshiba/codec";
import { Batch, Store } from "~/storage/Store.ts";
import { BlobStore, type BlobStoreBatch } from "./BlobStore.ts";

export interface ArrayStoreBatch<T> extends Batch {
	push(item: T): number;
	get(index: number): Promise<T>;
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

	private readonly _blob: BlobStore;
	private readonly _codec: T;

	private constructor(blob: BlobStore, options: ArrayStoreOptions<T>) {
		super();
		this._blob = blob;
		this._codec = options.codec;
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

	private get _stride(): number {
		return this._codec.stride.size;
	}

	length(): number {
		return this._blob.size() / this._stride;
	}

	async get(index: number): Promise<Codec.InferOutput<T>> {
		return this._blob.get(index * this._stride, this._codec);
	}

	async slice(start: number, end: number): Promise<Codec.InferOutput<T>[]> {
		return this._blob.get(start, new ArrayCodec(this._codec, { size: end - start }));
	}

	batch(): ArrayStoreBatch<Codec.InferOutput<T>> {
		const blob: BlobStoreBatch = this._blob.batch();
		const stride = this._stride;
		const codec = this._codec;

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

	async pin(): Promise<void> {
		return this._blob.pin();
	}

	async flush(): Promise<void> {
		return this._blob.flush();
	}

	async rollback(): Promise<void> {
		return this._blob.rollback();
	}

	async truncate(length: number): Promise<void> {
		return this._blob.truncate(length * this._stride);
	}

	close(): void {
		this._blob.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
