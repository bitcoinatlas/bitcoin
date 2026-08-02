import { ArrayCodec, type Codec, type FixedCodec } from "@nomadshiba/codec";
import { BlobStore } from "./BlobStore.ts";
import { Store } from "~/libs/storage/Store.ts";

export type ArrayStoreOptions<T extends FixedCodec> = {
	path: string;
	item: T;
	minChunkSize: number;
};

export class ArrayStore<T extends FixedCodec> extends Store implements Disposable {
	public readonly path: string;
	public readonly blob: BlobStore;
	public readonly codec: T;

	private constructor(blob: BlobStore, options: ArrayStoreOptions<T>) {
		super();
		this.blob = blob;
		this.codec = options.item;
		this.path = options.path;
	}

	static open<T extends FixedCodec>(options: ArrayStoreOptions<T>): ArrayStore<T> {
		if (options.minChunkSize < 0) {
			throw new RangeError(`minChunkSize must be non-negative, got ${options.minChunkSize}`);
		}
		if (!Number.isInteger(options.item.stride.size) || options.item.stride.size <= 0) {
			throw new RangeError(`stride.size must be a positive integer, got ${options.item.stride.size}`);
		}
		const blob = BlobStore.open({
			path: options.path,
			chunkSize: (Math.ceil(options.minChunkSize / options.item.stride.size) * options.item.stride.size) || options.item.stride.size,
		});
		return new ArrayStore(blob, options);
	}

	size(): number {
		return this.blob.size() / this.codec.stride.size;
	}

	reveal(size: number): void {
		return this.blob.reveal(size * this.codec.stride.size);
	}

	truncate(size: number): void {
		return this.blob.truncate(size * this.codec.stride.size);
	}

	next(offset = this.size()): number {
		return this.blob.next(this.codec.stride.size, offset * this.codec.stride.size) / this.codec.stride.size;
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
		const size = this.size();
		if (end > size) end = size;
		if (start < 0) {
			throw new RangeError(`slice out of bounds start=${start} end=${end} size=${size}`);
		}
		if (start > size) start = size;
		if (end <= start) return [];
		const [value] = await this.blob.getAsync(start * this.codec.stride.size, new ArrayCodec(this.codec, { size: end - start }));
		return value;
	}

	mmap(index: number) {
		return this.blob.mmap(index * this.codec.stride.size, this.codec.stride.size);
	}

	push(item: Codec.InferInput<T>): number {
		const pointer = this.blob.next(this.codec.stride.size);
		const size = pointer + this.blob.prepare(pointer, this.codec.encode(item));
		this.blob.reveal(size);
		return pointer / this.codec.stride.size;
	}

	prepare(index: number, item: Codec.InferInput<T>): void {
		const size = this.size();
		if (index < size) {
			throw new RangeError([
				`set index=${index} is behind the cursor (size=${size}).`,
				`set can only fill space at or in front of the cursor`,
			].join("\n"));
		}
		this.blob.prepare(index * this.codec.stride.size, this.codec.encode(item));
	}

	sync(): void {
		this.blob.sync();
	}

	close(): void {
		this.blob.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
