import { ArrayCodec, type Codec, type FixedCodec } from "@nomadshiba/codec";
import { BlobStore } from "./BlobStore.ts";
import { Store } from "~/libs/storage/Store.ts";

export type ArrayStoreOptions<T extends FixedCodec, C extends Codec<number>> = {
	path: string;
	item: T;
	counter: C;
	itemsPerChunk: number;
};

export class ArrayStore<T extends FixedCodec, C extends Codec<number>> extends Store implements Disposable {
	public readonly path: string;

	public readonly blob: BlobStore<T, C>;
	public readonly codec: T;

	private constructor(blob: BlobStore<T, C>, options: ArrayStoreOptions<T, C>) {
		super();
		this.blob = blob;
		this.codec = options.item;
		this.path = options.path;
	}

	static open<T extends FixedCodec, C extends Codec<number>>(options: ArrayStoreOptions<T, C>): ArrayStore<T, C> {
		const blob = BlobStore.open({
			path: options.path,
			counter: options.counter,
			entry: options.item,
			maxChunkSize: options.itemsPerChunk * options.item.stride.size,
		});
		return new ArrayStore(blob, options);
	}

	size(): number {
		return this.blob.size() / this.codec.stride.size;
	}

	get(index: number): Codec.InferOutput<T> | undefined {
		const length = this.size();
		if (index < 0) {
			throw new RangeError(`get out of bounds index=${index} length=${length}`);
		}
		if (index >= length) return undefined;
		const [value] = this.blob.get(index * this.codec.stride.size, this.codec);
		return value;
	}

	async getAsync(index: number): Promise<Codec.InferOutput<T> | undefined> {
		const length = this.size();
		if (index < 0) {
			throw new RangeError(`get out of bounds index=${index} length=${length}`);
		}
		if (index >= length) return undefined;
		const [value] = await this.blob.getAsync(index * this.codec.stride.size, this.codec).catch((reason) => {
			console.log(index, length);
			throw reason;
		});
		return value;
	}

	slice(start: number, end: number): Codec.InferOutput<T>[] {
		const length = this.size();
		if (end > length) end = length;
		if (start < 0) {
			throw new RangeError(`slice out of bounds start=${start} end=${end} length=${length}`);
		}
		if (start > length) start = length;
		if (end <= start) return [];
		const [value] = this.blob.get(start * this.codec.stride.size, new ArrayCodec(this.codec, { size: end - start }));
		return value;
	}

	async sliceAsync(start: number, end: number): Promise<Codec.InferOutput<T>[]> {
		const length = this.size();
		if (end > length) end = length;
		if (start < 0) {
			throw new RangeError(`slice out of bounds start=${start} end=${end} length=${length}`);
		}
		if (start > length) start = length;
		if (end <= start) return [];
		const [value] = await this.blob.getAsync(start * this.codec.stride.size, new ArrayCodec(this.codec, { size: end - start }));
		return value;
	}

	push(item: Codec.InferInput<T>): number {
		const pointer = this.blob.append(this.codec.encode(item));
		return pointer / this.codec.stride.size;
	}

	set(index: number, item: Codec.InferInput<T>): void {
		const length = this.size();
		if (index < length) {
			throw new RangeError(`set index=${index} is behind the cursor (length=${length}); set can only fill space at or in front of the cursor`);
		}
		this.blob.writeInto(index * this.codec.stride.size, this.codec.encode(item));
	}

	resize(length: number): void {
		return this.blob.resize(length * this.codec.stride.size);
	}

	close(): void {
		this.blob.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
