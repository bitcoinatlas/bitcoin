export type BloomFilterOptions = {
	/** Expected number of distinct keys. Drives the bit-array size. Exceeding this
	 * inflates the false-positive rate but never causes false negatives. */
	expectedItems: number;
	/** Target false-positive rate at `expectedItems`. Default 1%. */
	falsePositiveRate?: number;
};

/**
 * Plain (add-only) bloom filter over arbitrary byte keys.
 *
 * Answers "definitely not present" or "maybe present". Used to skip RocksDB
 * reads for keys that cannot be in the store. Because it is add-only it must
 * always remain a *superset* of the real key set — never remove from it, and
 * always repopulate it before serving reads (see KvStore.open).
 *
 * Deletes are not represented: a deleted key's bits stay set, so it lingers as
 * a false positive until the filter is rebuilt. For low-churn stores this is
 * negligible; for high-churn stores (constant delete pressure) the FP rate
 * climbs over the process lifetime and a rebuild (or a counting/cuckoo variant)
 * is needed — see notes in the message.
 */
export class BloomFilter {
	/** Bit array, one bit per slot, packed 8/byte. */
	public readonly bits: Uint8Array<ArrayBuffer>;
	/** Number of bits (m). */
	public readonly size: number;
	/** Number of hash probes (k). */
	public readonly hashes: number;

	private _count = 0;

	constructor(options: BloomFilterOptions) {
		const n = Math.max(1, options.expectedItems);
		const p = options.falsePositiveRate ?? 0.01;

		// m = ceil( -(n * ln p) / (ln2)^2 ), rounded up to a whole byte.
		const m = Math.ceil(-(n * Math.log(p)) / (Math.LN2 * Math.LN2));
		this.size = Math.ceil(m / 8) * 8;

		// Index math relies on `>>> 0` / `>>> 3`, which are uint32 ops, so the bit
		// count must stay below 2^32 (i.e. under 512 MiB). Sharding or a higher p
		// is the escape hatch past that.
		if (this.size >= 0x1_0000_0000) {
			throw new RangeError(
				`BloomFilter too large (${this.size} bits). Raise falsePositiveRate or shard the keyspace.`,
			);
		}

		// k = round( (m / n) * ln2 ), at least 1.
		this.hashes = Math.max(1, Math.round((this.size / n) * Math.LN2));
		this.bits = new Uint8Array(this.size >>> 3);
	}

	/** Approximate number of distinct keys added (does not dedupe). */
	get count(): number {
		return this._count;
	}

	/** Current estimated false-positive rate given how full the filter is. */
	get estimatedFalsePositiveRate(): number {
		return Math.pow(1 - Math.exp((-this.hashes * this._count) / this.size), this.hashes);
	}

	add(key: Uint8Array): void {
		const h1 = murmur3_32(key, 0x9747b28c);
		let h2 = murmur3_32(key, 0x85ebca6b);
		if (h2 === 0) h2 = 1; // keep double-hashing non-degenerate
		for (let i = 0; i < this.hashes; i++) {
			const idx = ((h1 + Math.imul(i, h2)) >>> 0) % this.size;
			this.bits[idx >>> 3]! |= 1 << (idx & 7);
		}
		this._count++;
	}

	/** false => definitely absent. true => possibly present. */
	mightContain(key: Uint8Array): boolean {
		const h1 = murmur3_32(key, 0x9747b28c);
		let h2 = murmur3_32(key, 0x85ebca6b);
		if (h2 === 0) h2 = 1;
		for (let i = 0; i < this.hashes; i++) {
			const idx = ((h1 + Math.imul(i, h2)) >>> 0) % this.size;
			if ((this.bits[idx >>> 3]! & (1 << (idx & 7))) === 0) return false;
		}
		return true;
	}

	clear(): void {
		this.bits.fill(0);
		this._count = 0;
	}
}

/** MurmurHash3 x86_32. Fast, good avalanche, no BigInt. */
function murmur3_32(key: Uint8Array, seed: number): number {
	let h = seed >>> 0;
	const len = key.length;
	const nblocks = len >> 2;

	let i = 0;
	for (let b = 0; b < nblocks; b++) {
		let k = (key[i]! | (key[i + 1]! << 8) | (key[i + 2]! << 16) | (key[i + 3]! << 24)) >>> 0;
		i += 4;
		k = Math.imul(k, 0xcc9e2d51);
		k = (k << 15) | (k >>> 17);
		k = Math.imul(k, 0x1b873593);
		h ^= k;
		h = (h << 13) | (h >>> 19);
		h = (Math.imul(h, 5) + 0xe6546b64) | 0;
	}

	let k = 0;
	switch (len & 3) {
		case 3:
			k ^= key[i + 2]! << 16;
		// falls through
		case 2:
			k ^= key[i + 1]! << 8;
		// falls through
		case 1:
			k ^= key[i]!;
			k = Math.imul(k, 0xcc9e2d51);
			k = (k << 15) | (k >>> 17);
			k = Math.imul(k, 0x1b873593);
			h ^= k;
	}

	h ^= len;
	h ^= h >>> 16;
	h = Math.imul(h, 0x85ebca6b);
	h ^= h >>> 13;
	h = Math.imul(h, 0xc2b2ae35);
	h ^= h >>> 16;
	return h >>> 0;
}
