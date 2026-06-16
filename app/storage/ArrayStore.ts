import type { Codec, FixedCodec } from "@nomadshiba/codec";
import type { Batch, Store } from "~/storage/Store.ts";
import { BlobStore, type BlobStoreBatch, type BlobStoreOptions } from "./BlobStore.ts";

export interface ArrayStoreBatch<T> extends Batch {
	append(item: T): number;
	get(index: number): Promise<T>;
	length(): number;
}

export type ArrayStoreOptions<T extends FixedCodec<any>> = BlobStoreOptions & {
	codec: T;
};

export class ArrayStore<T extends FixedCodec<any>> implements Store<ArrayStoreBatch<Codec.InferOutput<T>>>, Disposable {
	private readonly _blob: BlobStore;
	private readonly _codec: T;

	private constructor(blob: BlobStore, codec: T) {
		this._blob = blob;
		this._codec = codec;
	}

	static async open<T extends FixedCodec<any>>(options: ArrayStoreOptions<T>): Promise<ArrayStore<T>> {
		const { codec } = options;
		const blob = await BlobStore.open(options);
		return new ArrayStore(blob, codec);
	}

	private get _stride(): number {
		return this._codec.stride.size;
	}

	get length(): number {
		return this._blob.size() / this._stride;
	}

	async get(index: number): Promise<Codec.InferOutput<T>> {
		return this._blob.get(index * this._stride, this._codec);
	}

	batch(): ArrayStoreBatch<Codec.InferOutput<T>> {
		const blob: BlobStoreBatch = this._blob.batch();
		const stride = this._stride;
		const codec = this._codec;

		const length: ArrayStoreBatch<Codec.InferOutput<T>>["length"] = () => {
			return blob.size() / stride;
		};

		const append: ArrayStoreBatch<Codec.InferOutput<T>>["append"] = (item) => {
			const pointer = blob.append(codec.encode(item));
			return pointer / stride;
		};

		const get: ArrayStoreBatch<Codec.InferOutput<T>>["get"] = (index) => {
			return blob.get(index * stride, codec);
		};

		const apply: ArrayStoreBatch<Codec.InferOutput<T>>["apply"] = () => blob.apply();
		const discard: ArrayStoreBatch<Codec.InferOutput<T>>["discard"] = () => blob.discard();

		return { length, append, get, apply, discard };
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
