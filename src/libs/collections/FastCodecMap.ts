import type { Codec, FixedCodec } from "@nomadshiba/codec";

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

export interface FastCodecMapOptions<K extends FixedCodec, V extends FixedCodec> {
	key: K;
	value: V;
	capacity?: number;
}

const HEADER_U32 = 8;
const HEADER_BYTES = HEADER_U32 * 4;
const MAGIC = 0x42554d31; // "BUM1"

interface Layout {
	hashesOff: number;
	occOff: number;
	keysOff: number;
	valsOff: number;
	total: number;
}

// All four regions are sized by `capacity` and the codec strides, so the layout
// is fully determined by (capacity, keyLen, valLen). Everything past `hashes` is
// a byte region, so no alignment padding is needed; `hashes` sits at offset 32
// (4-aligned for its Uint32 view).
function layoutOf(capacity: number, keyLen: number, valLen: number): Layout {
	const hashesOff = HEADER_BYTES;
	const occOff = hashesOff + capacity * 4;
	const keysOff = occOff + capacity;
	const valsOff = keysOff + capacity * keyLen;
	const total = valsOff + capacity * valLen;
	return { hashesOff, occOff, keysOff, valsOff, total };
}

// Open-addressed (linear-probing) map keyed by a FixedCodec value, backed by ONE
// ArrayBuffer so it can cross workers by transfer (zero-copy, one entry in the
// transfer list).
//
// Constructed only via the static factories:
//   - create(...)     — a fresh, empty map.
//   - fromBuffer(...) — adopt a buffer received from another worker, supplying
//                       the same codecs (their strides are checked against the
//                       header, so the wrong codec fails loudly).
//
// Both key and value are FixedCodec: the dense per-slot layout needs a fixed
// stride for each. A variable-length value can't live here — store a fixed
// pointer (e.g. a U48 into a BlobStore) as the value instead. Keys/values are
// stored as their encoded bytes inline, so grow() is pure byte moves with no
// codec round-trip.
//
// set: append-only, no dedup scan (caller guarantees uniqueness). setOwned:
// overwrite-or-insert. No per-key delete (no tombstones — probing stops at the
// first empty slot). clear: wholesale.
//
// Layout in the buffer:
//   [ header(8×u32) | hashes(u32×cap) | occupied(u8×cap) | keys(cap×keyLen) | vals(cap×valLen) ]
// Occupancy is an explicit u8 array because a zeroed slot is a valid encoded
// entry, so there's no "undefined" sentinel.
export class FastCodecMap<K extends FixedCodec, V extends FixedCodec> {
	readonly key: K;
	readonly value: V;
	private readonly keyLen: number;
	private readonly valLen: number;
	// Reusable encode target for the query/insert key. Not part of `buffer`, not
	// transferred.
	private readonly keyScratch: Uint8Array<ArrayBuffer>;

	// Rebound on grow(), so not readonly.
	buffer!: ArrayBuffer;
	private header!: Uint32Array<ArrayBuffer>;
	private hashes!: Uint32Array<ArrayBuffer>;
	private occupied!: Uint8Array<ArrayBuffer>;
	private keyBytes!: Uint8Array<ArrayBuffer>;
	private valBytes!: Uint8Array<ArrayBuffer>;
	private capacity!: number;
	private mask!: number;
	private threshold!: number;
	private size_: number;

	// The buffer must already be sized for `capacity` (the factories handle that).
	// Header contents are (re)written here, so a freshly zeroed buffer is fine.
	private constructor(key: K, value: V, buffer: ArrayBuffer, capacity: number, size: number) {
		this.key = key;
		this.value = value;
		this.keyLen = key.stride.size;
		this.valLen = value.stride.size;
		this.keyScratch = new Uint8Array(this.keyLen);
		this.size_ = size;
		this.bind(buffer, capacity);
		this.writeHeader();
	}

	// A fresh, empty map.
	static create<K extends FixedCodec, V extends FixedCodec>(
		options: FastCodecMapOptions<K, V>,
	): FastCodecMap<K, V> {
		const keyLen = options.key.stride.size;
		const valLen = options.value.stride.size;
		const capacity = Math.pow(2, Math.ceil(Math.log2(Math.max(1, options.capacity ?? 16))));
		const buffer = new ArrayBuffer(layoutOf(capacity, keyLen, valLen).total);
		return new FastCodecMap<K, V>(options.key, options.value, buffer, capacity, 0);
	}

	// Adopt a buffer produced by takeBuffer() on another worker. The codecs must
	// be the same ones used to build it; their strides are checked against the
	// header.
	static fromBuffer<K extends FixedCodec, V extends FixedCodec>(
		buffer: ArrayBuffer,
		key: K,
		value: V,
	): FastCodecMap<K, V> {
		const header = new Uint32Array(buffer, 0, HEADER_U32);
		if (header[0] !== MAGIC) throw new Error("FastCodecMap: bad magic");
		if (header[5] !== key.stride.size) throw new Error("FastCodecMap: key codec stride mismatch");
		if (header[6] !== value.stride.size) throw new Error("FastCodecMap: value codec stride mismatch");
		return new FastCodecMap<K, V>(key, value, buffer, header[1]!, header[4]!);
	}

	// Point all mutable layout state at a buffer + capacity. Used by the
	// constructor and by grow(). Derives mask/threshold from capacity.
	private bind(buffer: ArrayBuffer, capacity: number): void {
		const L = layoutOf(capacity, this.keyLen, this.valLen);
		this.buffer = buffer;
		this.capacity = capacity;
		this.mask = capacity - 1;
		this.threshold = (capacity * LOAD) | 0;
		this.header = new Uint32Array(buffer, 0, HEADER_U32);
		this.hashes = new Uint32Array(buffer, L.hashesOff, capacity);
		this.occupied = new Uint8Array(buffer, L.occOff, capacity);
		this.keyBytes = new Uint8Array(buffer, L.keysOff, capacity * this.keyLen);
		this.valBytes = new Uint8Array(buffer, L.valsOff, capacity * this.valLen);
	}

	private writeHeader(): void {
		const h = this.header;
		h[0] = MAGIC;
		h[1] = this.capacity;
		h[2] = this.mask;
		h[3] = this.threshold;
		h[4] = this.size_;
		h[5] = this.keyLen;
		h[6] = this.valLen;
	}

	// Sync the mutable scalar (size) into the header and hand back the buffer for
	// a postMessage transfer list. After the buffer is transferred this instance
	// is dead (its views detach); the receiver calls fromBuffer with the codecs.
	takeBuffer(): ArrayBuffer {
		this.writeHeader();
		return this.buffer;
	}

	size(): number {
		return this.size_;
	}

	// Compare the already-encoded query key in `keyScratch` against slot bytes.
	private eqAt(slot: number, ks: Uint8Array): boolean {
		const keyLen = this.keyLen;
		const base = slot * keyLen;
		const kb = this.keyBytes;
		for (let i = 0; i < keyLen; i++) {
			if (kb[base + i] !== ks[i]) return false;
		}
		return true;
	}

	// Append a unique key/value. No existence check (caller guarantees the key is
	// new); a double-set inserts a second invisible slot.
	set(key: Codec.InferInput<K>, value: Codec.InferInput<V>): void {
		if (this.size_ >= this.threshold) this.grow();
		const ks = this.keyScratch;
		this.key.encodeInto(key, ks);
		const hash = hashKeyU32(ks);
		const occupied = this.occupied;
		const mask = this.mask;
		let slot = hash & mask;
		while (occupied[slot]) slot = (slot + 1) & mask;
		occupied[slot] = 1;
		this.hashes[slot] = hash;
		this.keyBytes.set(ks, slot * this.keyLen);
		const vo = slot * this.valLen;
		this.value.encodeInto(value, this.valBytes.subarray(vo, vo + this.valLen));
		this.size_++;
	}

	// Overwrite the value for an existing key, or append-only insert if absent.
	setOwned(key: Codec.InferInput<K>, value: Codec.InferInput<V>): void {
		const ks = this.keyScratch;
		this.key.encodeInto(key, ks);
		const hash = hashKeyU32(ks);
		const occupied = this.occupied;
		const hashes = this.hashes;
		const mask = this.mask;
		let slot = hash & mask;
		while (occupied[slot]) {
			if (hashes[slot] === hash && this.eqAt(slot, ks)) {
				const vo = slot * this.valLen;
				this.value.encodeInto(value, this.valBytes.subarray(vo, vo + this.valLen));
				return;
			}
			slot = (slot + 1) & mask;
		}
		if (this.size_ >= this.threshold) {
			this.grow();
			this.set(key, value);
			return;
		}
		occupied[slot] = 1;
		hashes[slot] = hash;
		this.keyBytes.set(ks, slot * this.keyLen);
		const vo = slot * this.valLen;
		this.value.encodeInto(value, this.valBytes.subarray(vo, vo + this.valLen));
		this.size_++;
	}

	get(key: Codec.InferInput<K>): Codec.InferOutput<V> | undefined {
		const ks = this.keyScratch;
		this.key.encodeInto(key, ks);
		const hash = hashKeyU32(ks);
		const occupied = this.occupied;
		const hashes = this.hashes;
		const mask = this.mask;
		let slot = hash & mask;
		while (occupied[slot]) {
			if (hashes[slot] === hash && this.eqAt(slot, ks)) {
				const vo = slot * this.valLen;
				return this.value.decodeValue(this.valBytes.subarray(vo, vo + this.valLen));
			}
			slot = (slot + 1) & mask;
		}
		return undefined;
	}

	has(key: Codec.InferInput<K>): boolean {
		return this.get(key) !== undefined;
	}

	// Allocate a 2x buffer and rehash. Pure byte moves — no codec round-trip,
	// since both keys and values are already in their encoded form. The old buffer
	// is discarded; build + grow fully in one worker, then takeBuffer() once.
	private grow(): void {
		const oldCap = this.capacity;
		const keyLen = this.keyLen;
		const valLen = this.valLen;
		const oldOcc = this.occupied;
		const oldHashes = this.hashes;
		const oldKeys = this.keyBytes;
		const oldVals = this.valBytes;

		const newBuf = new ArrayBuffer(layoutOf(oldCap * 2, keyLen, valLen).total);
		this.bind(newBuf, oldCap * 2);

		const occupied = this.occupied;
		const hashes = this.hashes;
		const keyBytes = this.keyBytes;
		const valBytes = this.valBytes;
		const mask = this.mask;
		for (let i = 0; i < oldCap; i++) {
			if (!oldOcc[i]) continue;
			const hash = oldHashes[i]!;
			let slot = hash & mask;
			while (occupied[slot]) slot = (slot + 1) & mask;
			occupied[slot] = 1;
			hashes[slot] = hash;
			keyBytes.set(oldKeys.subarray(i * keyLen, i * keyLen + keyLen), slot * keyLen);
			valBytes.set(oldVals.subarray(i * valLen, i * valLen + valLen), slot * valLen);
		}
	}

	// Empty every slot, keeping the grown capacity.
	clear(): void {
		this.occupied.fill(0);
		this.hashes.fill(0);
		this.keyBytes.fill(0);
		this.valBytes.fill(0);
		this.size_ = 0;
	}

	[Symbol.iterator](): Generator<[Codec.InferOutput<K>, Codec.InferOutput<V>]> {
		return this.entries();
	}

	// Decodes both key and value per entry (the codecs own whether that allocates
	// or returns a view into the buffer — slice byte outputs to keep them past a
	// grow/clear/transfer).
	*entries(): Generator<[Codec.InferOutput<K>, Codec.InferOutput<V>]> {
		const occupied = this.occupied;
		const keyLen = this.keyLen;
		const valLen = this.valLen;
		for (let i = 0; i < this.capacity; i++) {
			if (!occupied[i]) continue;
			const ko = i * keyLen;
			const vo = i * valLen;
			yield [
				this.key.decodeValue(this.keyBytes.subarray(ko, ko + keyLen)),
				this.value.decodeValue(this.valBytes.subarray(vo, vo + valLen)),
			];
		}
	}

	*keys(): Generator<Codec.InferOutput<K>> {
		const occupied = this.occupied;
		const keyLen = this.keyLen;
		for (let i = 0; i < this.capacity; i++) {
			if (!occupied[i]) continue;
			const ko = i * keyLen;
			yield this.key.decodeValue(this.keyBytes.subarray(ko, ko + keyLen));
		}
	}

	*values(): Generator<Codec.InferOutput<V>> {
		const occupied = this.occupied;
		const valLen = this.valLen;
		for (let i = 0; i < this.capacity; i++) {
			if (!occupied[i]) continue;
			const vo = i * valLen;
			yield this.value.decodeValue(this.valBytes.subarray(vo, vo + valLen));
		}
	}
}
