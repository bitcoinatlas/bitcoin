import { equals } from "@std/bytes";

function hashKeyU32(key: Uint8Array): number {
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

// Linear probing degrades past ~0.7 load, so keep headroom over the 0.75 a
// chained map can tolerate.
const LOAD = 0.7;

// Open-addressed (linear-probing) map keyed by Uint8Array.
//
// Append-only + no-copy + no-delete, which is what makes it fast and simple:
//   - keys are stored BY REFERENCE (no defensive copy). Safe only when keys are
//     immutable after insertion — e.g. block hashes. Don't hand it a buffer you
//     later mutate.
//   - `put` never checks for an existing key (no dedup scan). Caller guarantees
//     uniqueness; a double-put inserts a second slot the lookups can't see.
//   - no per-key delete -> no tombstones -> probing stops cleanly at the first
//     empty slot. Shrink/reset is wholesale via `clear`.
//
// Storage is parallel arrays: a Uint32Array of cached hashes (cache-friendly
// linear scan) and a plain array of key references — no [hash,key,value] tuple
// is allocated per entry, which is where the chained version spent its time.
//
// An empty slot is `keys[slot] === undefined`. That doubles as the occupancy
// flag, so no separate bitset is needed.
export class FastUint8ArrayMap<V> implements Iterable<[Uint8Array, V]> {
	private _keys: Array<Uint8Array | undefined>;
	private _hashes: Uint32Array;
	private _vals: Array<V | undefined>;
	private _mask: number;
	private _threshold: number;
	private _size = 0;

	constructor(capacity = 16) {
		const pow2 = Math.pow(2, Math.ceil(Math.log2(Math.max(1, capacity))));
		this._keys = new Array(pow2);
		this._hashes = new Uint32Array(pow2);
		this._vals = new Array(pow2);
		this._mask = pow2 - 1;
		this._threshold = (pow2 * LOAD) | 0;
	}

	[Symbol.iterator](): Iterator<[Uint8Array, V]> {
		return this.entries();
	}

	size() {
		return this._size;
	}

	// Append a unique key/value. No existence check, no copy.
	set(key: Uint8Array, value: V): void {
		if (this._size >= this._threshold) this.grow();
		const hash = hashKeyU32(key);
		let slot = hash & this._mask;
		while (this._keys[slot] !== undefined) slot = (slot + 1) & this._mask;
		this._keys[slot] = key;
		this._hashes[slot] = hash;
		this._vals[slot] = value;
		this._size++;
	}

	get(key: Uint8Array): V | undefined {
		const hash = hashKeyU32(key);
		const keys = this._keys;
		const hashes = this._hashes;
		const mask = this._mask;
		let slot = hash & mask;
		let k: Uint8Array | undefined;
		while ((k = keys[slot]) !== undefined) {
			if (hashes[slot] === hash && equals(k, key)) return this._vals[slot];
			slot = (slot + 1) & mask;
		}
		return undefined;
	}

	has(key: Uint8Array): boolean {
		return this.get(key) !== undefined;
	}

	private grow(): void {
		const oldKeys = this._keys;
		const oldHashes = this._hashes;
		const oldVals = this._vals;
		const cap = oldKeys.length * 2;

		this._keys = new Array(cap);
		this._hashes = new Uint32Array(cap);
		this._vals = new Array(cap);
		this._mask = cap - 1;
		this._threshold = (cap * LOAD) | 0;

		const mask = this._mask;
		for (let i = 0; i < oldKeys.length; i++) {
			const k = oldKeys[i];
			if (k === undefined) continue;
			const hash = oldHashes[i]!;
			let slot = hash & mask;
			while (this._keys[slot] !== undefined) slot = (slot + 1) & mask;
			this._keys[slot] = k;
			this._hashes[slot] = hash;
			this._vals[slot] = oldVals[i];
		}
	}

	// Empty every slot. Keeps the grown capacity so a reorg reindex refills
	// without immediately re-growing.
	clear(): void {
		this._keys.fill(undefined);
		this._vals.fill(undefined);
		this._hashes.fill(0);
		this._size = 0;
	}

	*entries(): Generator<[Uint8Array, V]> {
		const keys = this._keys;
		for (let i = 0; i < keys.length; i++) {
			const k = keys[i];
			if (k !== undefined) yield [k, this._vals[i]!];
		}
	}

	*keys(): Generator<Uint8Array> {
		const keys = this._keys;
		for (let i = 0; i < keys.length; i++) {
			const k = keys[i];
			if (k !== undefined) yield k;
		}
	}

	*values(): Generator<V> {
		const keys = this._keys;
		for (let i = 0; i < keys.length; i++) {
			const k = keys[i];
			if (k !== undefined) yield this._vals[i]!;
		}
	}
}
