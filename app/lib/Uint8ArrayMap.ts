import { compare } from "~/lib/utils/bytes.ts";

/**
 * Fast Uint8Array hash map using value equality
 * Variable-length key support
 */
export class Uint8ArrayMap<V> {
	private buckets: Array<Array<[Uint8Array, V]>>;
	private mask: number;
	private _size = 0;
	public get size() {
		return this._size;
	}

	constructor(capacity = 16384) {
		const pow2 = Math.pow(2, Math.ceil(Math.log2(capacity)));
		this.mask = pow2 - 1;
		this.buckets = new Array(pow2);
		for (let i = 0; i < pow2; i++) {
			this.buckets[i] = [];
		}
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
			if (!compare(bKey, key)) continue;
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
			if (!compare(bKey, key)) continue;
			entry[1] = value;
			return;
		}

		bucket.push([key.slice(), value]);
		this._size++;
	}

	delete(key: Uint8Array): boolean {
		const hash = this.hash(key);
		const bucket = this.buckets[hash & this.mask]!;

		for (let i = 0; i < bucket.length; i++) {
			const [bKey] = bucket[i]!;
			if (!compare(bKey, key)) continue;
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
