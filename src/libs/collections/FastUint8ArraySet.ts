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

// Open-addressed (linear-probing) set keyed by Uint8Array.
//
// Append-only + no-copy + no-delete, which is what makes it fast and simple:
//   - keys are stored BY REFERENCE (no defensive copy). Safe only when keys are
//     immutable after insertion — e.g. block hashes. Don't hand it a buffer you
//     later mutate.
//   - `add` checks membership while probing (a set must dedup), so a re-add is a
//     cheap no-op rather than a second hidden slot. This is the one place it
//     diverges from the map, whose `set` skips the existence scan.
//   - no per-key delete -> no tombstones -> probing stops cleanly at the first
//     empty slot. Shrink/reset is wholesale via `clear`.
//
// Storage is parallel arrays: a Uint32Array of cached hashes (cache-friendly
// linear scan) and a plain array of key references — no [hash,key] tuple is
// allocated per entry, which is where the chained version spent its time.
//
// An empty slot is `keys[slot] === undefined`. That doubles as the occupancy
// flag, so no separate bitset is needed.
export class FastUint8ArraySet implements Iterable<Uint8Array> {
	private keySlots: Array<Uint8Array | undefined>;
	private hashes: Uint32Array;
	private mask: number;
	private threshold: number;
	private size_ = 0;

	constructor(capacity = 16) {
		const pow2 = Math.pow(2, Math.ceil(Math.log2(Math.max(1, capacity))));
		this.keySlots = new Array(pow2);
		this.hashes = new Uint32Array(pow2);
		this.mask = pow2 - 1;
		this.threshold = (pow2 * LOAD) | 0;
	}

	[Symbol.iterator](): Iterator<Uint8Array> {
		return this.values();
	}

	size() {
		return this.size_;
	}

	// Insert a key, deduping by reference-independent value equality. Returns
	// true if newly added, false if it was already present. No copy.
	add(key: Uint8Array): boolean {
		if (this.size_ >= this.threshold) this.grow();
		const hash = hashKeyU32(key);
		const keySlots = this.keySlots;
		const hashes = this.hashes;
		const mask = this.mask;
		let slot = hash & mask;
		let k: Uint8Array | undefined;
		while ((k = keySlots[slot]) !== undefined) {
			if (hashes[slot] === hash && equals(k, key)) return false;
			slot = (slot + 1) & mask;
		}
		keySlots[slot] = key;
		hashes[slot] = hash;
		this.size_++;
		return true;
	}

	has(key: Uint8Array): boolean {
		const hash = hashKeyU32(key);
		const keySlots = this.keySlots;
		const hashes = this.hashes;
		const mask = this.mask;
		let slot = hash & mask;
		let k: Uint8Array | undefined;
		while ((k = keySlots[slot]) !== undefined) {
			if (hashes[slot] === hash && equals(k, key)) return true;
			slot = (slot + 1) & mask;
		}
		return false;
	}

	private grow(): void {
		const oldKeys = this.keySlots;
		const oldHashes = this.hashes;
		const cap = oldKeys.length * 2;

		this.keySlots = new Array(cap);
		this.hashes = new Uint32Array(cap);
		this.mask = cap - 1;
		this.threshold = (cap * LOAD) | 0;

		const mask = this.mask;
		for (let i = 0; i < oldKeys.length; i++) {
			const k = oldKeys[i];
			if (k === undefined) continue;
			const hash = oldHashes[i]!;
			let slot = hash & mask;
			while (this.keySlots[slot] !== undefined) slot = (slot + 1) & mask;
			this.keySlots[slot] = k;
			this.hashes[slot] = hash;
		}
	}

	// Empty every slot. Keeps the grown capacity so a reorg reindex refills
	// without immediately re-growing.
	clear(): void {
		this.keySlots.fill(undefined);
		this.hashes.fill(0);
		this.size_ = 0;
	}

	*values(): Generator<Uint8Array> {
		const keySlots = this.keySlots;
		for (let i = 0; i < keySlots.length; i++) {
			const k = keySlots[i];
			if (k !== undefined) yield k;
		}
	}

	// Alias to mirror the map's surface, where callers may expect `keys()`.
	keys(): Generator<Uint8Array> {
		return this.values();
	}
}
