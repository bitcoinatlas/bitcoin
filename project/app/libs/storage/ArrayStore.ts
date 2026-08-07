import { ArrayCodec, type Codec, type FixedCodec } from "@nomadshiba/codec";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { Store } from "~/libs/storage/Store.ts";

export type ArrayStoreOptions<T extends FixedCodec> = {
	path: string;
	item: T;
	minChunkSize: number;
};

export class ArrayStore<T extends FixedCodec> extends Store implements Disposable {
	public readonly path: string;
	public readonly blob: BlobStore;
	public readonly item: T;

	private constructor(blob: BlobStore, options: ArrayStoreOptions<T>) {
		super();
		this.blob = blob;
		this.item = options.item;
		this.path = options.path;
	}

	public static open<T extends FixedCodec>(options: ArrayStoreOptions<T>): ArrayStore<T> {
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

	public size(): number {
		return this.blob.size() / this.item.stride.size;
	}

	public reveal(size: number, isBroadcast?: boolean): void {
		return this.blob.reveal(size * this.item.stride.size, isBroadcast);
	}

	public truncate(size: number): void {
		return this.blob.truncate(size * this.item.stride.size);
	}

	public get(index: number): Codec.InferOutput<T> | undefined {
		const length = this.size();
		if (index < 0) {
			throw new RangeError(`get out of bounds index=${index} length=${length}`);
		}
		if (index >= length) return undefined;
		const [value] = this.blob.get(index * this.item.stride.size, this.item);
		return value;
	}

	public async getAsync(index: number): Promise<Codec.InferOutput<T> | undefined> {
		const length = this.size();
		if (index < 0) {
			throw new RangeError(`get out of bounds index=${index} length=${length}`);
		}
		if (index >= length) return undefined;
		const [value] = await this.blob.getAsync(index * this.item.stride.size, this.item).catch((reason) => {
			console.log(index, length);
			throw reason;
		});
		return value;
	}

	public slice(start: number, end: number): Codec.InferOutput<T>[] {
		const length = this.size();
		if (end > length) end = length;
		if (start < 0) {
			throw new RangeError(`slice out of bounds start=${start} end=${end} length=${length}`);
		}
		if (start > length) start = length;
		if (end <= start) return [];
		const [value] = this.blob.get(start * this.item.stride.size, new ArrayCodec(this.item, { size: end - start }));
		return value;
	}

	public async sliceAsync(start: number, end: number): Promise<Codec.InferOutput<T>[]> {
		const size = this.size();
		if (end > size) end = size;
		if (start < 0) {
			throw new RangeError(`slice out of bounds start=${start} end=${end} size=${size}`);
		}
		if (start > size) start = size;
		if (end <= start) return [];
		const [value] = await this.blob.getAsync(start * this.item.stride.size, new ArrayCodec(this.item, { size: end - start }));
		return value;
	}

	public mmap(index: number) {
		// chunkSize is always a multiple of stride.size (enforced in open()), so an
		// item at this offset never straddles a chunk boundary — begin can be the
		// raw index*stride offset directly, no next() needed.
		return this.blob.mmap(this.item.stride.size, index * this.item.stride.size);
	}

	public stage(item: Codec.InferInput<T>, index?: number): number {
		const size = this.size();
		index ??= size;
		if (index < size) {
			throw new RangeError([
				`set index=${index} is behind the cursor (size=${size}).`,
				`set can only fill space at or in front of the cursor`,
			].join(" "));
		}
		this.item.encodeInto(item, this.mmap(index));
		return index;
	}

	public sync(): void {
		this.blob.sync();
	}

	public close(): void {
		this.blob.close();
	}

	public [Symbol.dispose](): void {
		this.close();
	}
}
