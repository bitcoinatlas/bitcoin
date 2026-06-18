export type Uint8ArrayPoolOptions = {
	maxRetainedBytes?: number;
	maxBufferBytes?: number;
};

export class Uint8ArrayPoolNode extends Uint8Array<ArrayBuffer> implements Disposable {
	readonly capacity: number;
	#pool: Uint8ArrayPool | undefined;

	constructor(pool: Uint8ArrayPool, buffer: ArrayBuffer, length: number) {
		super(buffer, 0, length);
		this.capacity = buffer.byteLength;
		this.#pool = pool;
	}

	release(): void {
		this[Symbol.dispose]();
	}

	[Symbol.dispose](): void {
		const pool = this.#pool;
		if (!pool) return;
		this.#pool = undefined;
		pool.release(this.buffer);
	}
}

export class Uint8ArrayPool {
	readonly maxRetainedBytes: number;
	readonly maxBufferBytes: number;

	#buckets = new Map<number, ArrayBuffer[]>();
	#retainedBytes = 0;

	constructor(options: Uint8ArrayPoolOptions = {}) {
		this.maxRetainedBytes = options.maxRetainedBytes ?? 64 * 1024 * 1024;
		this.maxBufferBytes = options.maxBufferBytes ?? this.maxRetainedBytes;
	}

	get retainedBytes(): number {
		return this.#retainedBytes;
	}

	take(length: number): Uint8ArrayPoolNode {
		if (!Number.isSafeInteger(length) || length < 0) {
			throw new RangeError(`invalid pooled Uint8Array length=${length}`);
		}

		const capacity = bucketCapacity(length);
		const bucket = this.#buckets.get(capacity);
		const buffer = bucket?.pop() ?? new ArrayBuffer(capacity);
		if (bucket) {
			this.#retainedBytes -= capacity;
			if (bucket.length === 0) this.#buckets.delete(capacity);
		}

		return new Uint8ArrayPoolNode(this, buffer, length);
	}

	release(buffer: ArrayBuffer): void {
		const capacity = buffer.byteLength;
		if (capacity > this.maxBufferBytes || this.#retainedBytes + capacity > this.maxRetainedBytes) return;

		let bucket = this.#buckets.get(capacity);
		if (!bucket) this.#buckets.set(capacity, bucket = []);
		bucket.push(buffer);
		this.#retainedBytes += capacity;
	}

	clear(): void {
		this.#buckets.clear();
		this.#retainedBytes = 0;
	}
}

function bucketCapacity(length: number): number {
	if (length <= 0) return 0;
	return 2 ** Math.ceil(Math.log2(length));
}
