import { equals } from "@std/bytes";

type Bucket<V> = Array<[number, Uint8Array, V]>; // [hash, key, value]

export class Uint8ArrayMap<V> implements Iterable<[Uint8Array, V]> {
	private buckets: Array<Bucket<V>>;
	private mask: number;
	private threshold: number;
	private _size = 0;
	public get size() {
		return this._size;
	}

	constructor(capacity = 1) {
		const pow2 = Math.pow(2, Math.ceil(Math.log2(capacity)));
		this.mask = pow2 - 1;
		this.threshold = pow2 * 0.75;
		this.buckets = new Array(pow2);
		for (let i = 0; i < pow2; i++) this.buckets[i] = [];
	}

	[Symbol.iterator](): Iterator<[Uint8Array, V]> {
		return this.entries();
	}

	private hash(key: Uint8Array): number {
		const len = key.length < 32 ? key.length : 32;
		const end = len & ~3; // largest multiple of 4 <= len
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

	get(key: Uint8Array): V | undefined {
		const hash = this.hash(key);
		const bucket = this.buckets[hash & this.mask]!;
		for (let i = 0; i < bucket.length; i++) {
			const entry = bucket[i]!;
			if (entry[0] === hash && equals(entry[1], key)) return entry[2];
		}
		return undefined;
	}

	set(key: Uint8Array, value: V): void {
		const hash = this.hash(key);
		const bucket = this.buckets[hash & this.mask]!;
		for (let i = 0; i < bucket.length; i++) {
			const entry = bucket[i]!;
			if (entry[0] === hash && equals(entry[1], key)) {
				entry[2] = value;
				return;
			}
		}
		bucket.push([hash, key.slice(), value]);
		this._size++;
		if (this._size > this.threshold) this.rehash(this.buckets.length * 2);
	}

	private rehash(newCapacity: number): void {
		const newBuckets: Array<Bucket<V>> = new Array(newCapacity);
		for (let i = 0; i < newCapacity; i++) newBuckets[i] = [];
		const newMask = newCapacity - 1;
		const old = this.buckets;
		for (let b = 0; b < old.length; b++) {
			const bucket = old[b]!;
			for (let i = 0; i < bucket.length; i++) {
				const entry = bucket[i]!;
				newBuckets[entry[0] & newMask]!.push(entry); // reuse stored hash
			}
		}
		this.buckets = newBuckets;
		this.mask = newMask;
		this.threshold = newCapacity * 0.75;
	}

	delete(key: Uint8Array): boolean {
		const hash = this.hash(key);
		const bucket = this.buckets[hash & this.mask]!;
		for (let i = 0; i < bucket.length; i++) {
			const entry = bucket[i]!;
			if (entry[0] === hash && equals(entry[1], key)) {
				const last = bucket.length - 1;
				if (i !== last) bucket[i] = bucket[last]!; // swap-remove, O(1)
				bucket.pop();
				this._size--;
				return true;
			}
		}
		return false;
	}

	has(key: Uint8Array): boolean {
		return this.get(key) !== undefined;
	}

	clear(): void {
		for (let i = 0; i < this.buckets.length; i++) this.buckets[i] = [];
		this._size = 0;
	}

	*entries(): Generator<[Uint8Array, V]> {
		const buckets = this.buckets;
		for (let b = 0; b < buckets.length; b++) {
			const bucket = buckets[b]!;
			for (let i = 0; i < bucket.length; i++) {
				const entry = bucket[i]!;
				yield [entry[1], entry[2]];
			}
		}
	}

	*keys(): Generator<Uint8Array> {
		for (const [key] of this.entries()) yield key;
	}

	*values(): Generator<V> {
		for (const [, value] of this.entries()) yield value;
	}
}
