import { compare } from "~/utils/bytes.ts";

export class Uint8ArrayMap<V> implements Iterable<[Uint8Array, V]> {
	private buckets: Array<Array<[Uint8Array, V]>>;
	private mask: number;
	private _size = 0;
	public get size() {
		return this._size;
	}

	constructor(capacity = 1) {
		const pow2 = Math.pow(2, Math.ceil(Math.log2(capacity)));
		this.mask = pow2 - 1;
		this.buckets = new Array(pow2);
		for (let i = 0; i < pow2; i++) {
			this.buckets[i] = [];
		}
	}

	[Symbol.iterator](): Iterator<[Uint8Array, V], any, any> {
		return this.entries();
	}

	private hash(key: Uint8Array): number {
		let h = 0;
		for (let i = 0; i < Math.min(key.length, 32); i++) {
			h = ((h << 5) - h + key[i]!) | 0;
		}
		return (h >>> 0);
	}

	get(key: Uint8Array): V | undefined {
		const hash = this.hash(key);
		const bucket = this.buckets[hash & this.mask]!;

		for (let i = 0; i < bucket.length; i++) {
			const [bKey, bValue] = bucket[i]!;
			if (compare(bKey, key) !== 0) continue;
			return bValue;
		}
		return undefined;
	}

	set(key: Uint8Array, value: V): void {
		const hash = this.hash(key);
		const idx = hash & this.mask;
		const bucket = this.buckets[idx]!;

		for (let i = 0; i < bucket.length; i++) {
			const entry = bucket[i]!;
			const [bKey] = entry;
			if (compare(bKey, key) !== 0) continue;
			entry[1] = value;
			return;
		}

		bucket.push([key.slice(), value]);
		this._size++;

		// Rehash when load factor > 0.75
		if (this._size > this.buckets.length * 0.75) {
			this.rehash(this.buckets.length * 2);
		}
	}

	private rehash(newCapacity: number): void {
		const newBuckets: Array<Array<[Uint8Array, V]>> = new Array(newCapacity);
		for (let i = 0; i < newCapacity; i++) newBuckets[i] = [];
		const newMask = newCapacity - 1;
		for (const bucket of this.buckets) {
			for (const entry of bucket) {
				const h = this.hash(entry[0]) & newMask;
				newBuckets[h]!.push(entry);
			}
		}
		this.buckets = newBuckets;
		this.mask = newMask;
	}

	delete(key: Uint8Array): boolean {
		const hash = this.hash(key);
		const bucket = this.buckets[hash & this.mask]!;

		for (let i = 0; i < bucket.length; i++) {
			const [bKey] = bucket[i]!;
			if (compare(bKey, key) !== 0) continue;
			bucket.splice(i, 1);
			this._size--;
			return true;
		}
		return false;
	}

	has(key: Uint8Array): boolean {
		return this.get(key) !== undefined;
	}

	clear(): void {
		for (let i = 0; i < this.buckets.length; i++) {
			this.buckets[i] = [];
		}
		this._size = 0;
	}

	*entries(): Generator<[Uint8Array, V]> {
		for (const bucket of this.buckets) {
			for (const entry of bucket) {
				yield entry;
			}
		}
	}

	*keys(): Generator<Uint8Array> {
		for (const [key] of this.entries()) {
			yield key;
		}
	}

	*values(): Generator<V> {
		for (const [, value] of this.entries()) {
			yield value;
		}
	}
}
