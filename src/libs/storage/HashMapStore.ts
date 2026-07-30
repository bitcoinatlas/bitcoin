import { Bool, Codec, FixedCodec, StructCodec, TupleCodec, TupleOutput } from "@nomadshiba/codec";
import { Mmap } from "@nomadshiba/mmap";
import { equals } from "@std/bytes/equals";
import { join } from "@std/path";
import { Store } from "~/libs/storage/Store.ts";

/**
 * A memory mapping paired with its `bytes()` view, taken once at open time and
 * reused for every access. `Mmap.bytes()` allocates a fresh `Uint8Array` (over
 * a fresh `ArrayBuffer`) on every call, so caching it here keeps the hot read/
 * write loops allocation-free. The two always travel together: whenever the
 * mapping is (re)opened the `bytes` snapshot is refreshed alongside it.
 */
type MappedFile = { mapping: Mmap; bytes: Uint8Array };

function mapFile(path: string): MappedFile {
	const mapping = Mmap.openSync(path, { write: true });
	return { mapping, bytes: mapping.bytes() };
}

export type LoadFactorOptions = {
	/** Target average entries per bucket (`entryCount / bucketCount`) — same meaning as e.g. Java `HashMap`'s load factor. `1` aims for ~1 entry/bucket, `4` aims for ~4 (fewer, cheaper buckets; longer chains). Must be > 0. */
	target: number;
	/** How far the live load factor may drift from `target` before a rehash. Must be >= 0. */
	maxDrift: number;
};

export type GrowthOptions = {
	/** WHEN to grow: whenever the entries file's free space (physical − logical size) would drop below this many bytes — including at `open()` (a brand-new file has 0 bytes, so creation itself triggers a grow). Must be an integer >= 0; `0` = grow only when full. */
	headroom: number;
	/** HOW MUCH to grow: bytes added to the entries file's physical size per grow (repeated until the pending write fits with headroom to spare). Must be an integer > 0. */
	amount: number;
};

export type HashMapStoreOptions<Pointer extends FixedCodec<number>, Key extends Codec, Value extends Codec> = {
	path: string;
	pointer: Pointer;
	key: Key;
	value: Value;
	loadFactor: LoadFactorOptions;
	/** Pre-growth policy for the entries file. */
	growth: GrowthOptions;
};

/**
 * On-disk hash map with its own mmap'd buckets + entries files (no BlobStore,
 * no ArrayStore, no chunks, no compression).
 *
 * ## Layout
 * - `entries` (`path/entries`): append-only log. Each entry is
 *   `previous ++ key ++ value` (a struct). `previous` is the fixed-width first
 *   field so it can be patched in place during rehash. Links backwards to the
 *   previous entry in the same bucket (per-bucket singly-linked list).
 *   Kept ahead of its logical size by the `growth` policy (see options);
 *   the logical size is tracked in `meta`.
 * - `buckets` (`path/buckets`): flat array of fixed-width pointer slots.
 *   `buckets[hash(key) % count]` = head (newest entry) of that bucket.
 * - `meta` (`path/meta`): `{ stale, entriesSize, entriesCount }`. `stale` is
 *   set during rehash for crash safety; `entriesSize` is the entries file's
 *   logical size (bytes); `entriesCount` is the number of entries, kept for
 *   the load-factor math (see `loadFactor`) since entries are variable-length
 *   and byte size alone is a poor proxy for chain length.
 *
 * All three files are created in `open()` — a brand-new entries file has 0
 * bytes, so the normal `growth` rule grows it right there (buckets start at
 * `initialBuckets`) — so the memory mappings exist for the store's whole
 * lifetime, never absent.
 *
 * ## Pointers
 * Persisted pointers (bucket slots and the inline `previous` field) are stored
 * +1, so 0 = empty slot / end of chain. That +1 form is purely internal: every
 * public method (`put`, `getPointer`, `getValueAndPointer`, `getEntry`,
 * `getKey`, `setValue`, `putValue`) speaks in raw 0-based byte offsets into
 * the entries log. Note that offset `0` is a valid pointer (the first entry)
 * — always test lookup results with `=== undefined`, never truthiness.
 *
 * ## Load factor
 * `loadFactor.target` is the standard `entryCount / bucketCount` ratio (same
 * meaning as e.g. Java `HashMap`'s load factor) — NOT buckets per entry.
 * `bucketCount` is kept at `entryCount / loadFactor.target`, so `target = 1`
 * aims for ~1 entry/bucket, `target = 4` aims for ~4 (fewer, cheaper buckets,
 * longer chains). `loadFactor.maxDrift` bounds how far the live value may
 * wander before a rehash corrects it.
 *
 * ## Rehash
 * Replays entries oldest-first, patching inline `previous` links and rebuilding
 * heads. Also recomputes `entriesCount` from scratch (see "Load factor" above)
 * — it's the one place that field is allowed to go stale (`resize()` shrinking
 * truncates by byte offset alone) and gets corrected. Crash-safe via the
 * `stale` flag — a crash mid-rehash is detected on `open` and re-run. Current
 * sizes are flushed to meta BEFORE `stale` is set, so the recovery rescan
 * always covers every entry that was reachable when the rehash started.
 *
 * ## Crash safety of `put`
 * `put` writes, in order: entry bytes → bucket head → meta (`entriesSize` /
 * `entriesCount`). A crash before the head update leaves uncommitted bytes
 * past `entriesSize` — unreachable, and the next `put` simply overwrites
 * them. A crash after the head update but before meta is flushed leaves a
 * head pointing past `entriesSize`; `open` detects this in `recoverTail()`
 * (cheap check of bucket heads first) and walks the chains to extend
 * `entriesSize` / `entriesCount` over the committed tail.
 *
 * ## Staging bytes ahead of the log (`putValue` + growing `resize`)
 * `putValue(pointer, value)` writes an encoded value at an absolute offset at
 * or past `entriesSize` — without updating meta or buckets. Staged bytes
 * become live only when `resize()` grows `entriesSize` to cover them: the new
 * region must decode as a sequence of whole entries landing exactly on the
 * new size, then EVERYTHING is rehashed, hashing and bulk-indexing the staged
 * entries. If you never grow over staged bytes, the next `put` appends at
 * `entriesSize` and overwrites them.
 *
 * Shrinking `resize(size)` only truncates the logical view: `previous` links
 * always point backwards (to older entries), so surviving chains stay intact
 * and only dangling bucket heads are re-pointed to the newest surviving entry
 * of their chain. `size` must land on an entry boundary (e.g. a value earlier
 * returned by `size()`); a mid-entry cut is detected and rejected before
 * anything is modified.
 */
export class HashMapStore<Pointer extends FixedCodec<number>, Key extends Codec, Value extends Codec> extends Store implements Disposable {
	public readonly path: string;

	private readonly pointer: Pointer;
	private readonly key: Key;
	private readonly value: Value;
	private readonly entry: TupleCodec<[key: Key, value: Value]>;
	private readonly header: StructCodec<{ previous: Pointer; key: Key }>;
	private readonly item: StructCodec<{ previous: Pointer; key: Key; value: Value }>;
	private readonly meta: StructCodec<{ stale: typeof Bool; entriesSize: Pointer; entriesCount: Pointer }>;
	private readonly targetLoadFactor: number;
	private readonly maxLoadFactorDrift: number;
	private readonly growthHeadroom: number;
	private readonly growthAmount: number;

	private readonly entriesPath: string;
	private readonly bucketsPath: string;
	private readonly metaPath: string;
	private readonly metaSize: number;

	// Assigned in open() — every file is created there, so the mappings exist
	// for the store's whole lifetime and are never absent.
	private entriesMap!: MappedFile;
	private entriesPhysicalSize = 0;
	private entriesSize = 0;
	// Number of entries (not bytes) — used for the bucket-ratio math instead
	// of entriesSize, since entries are variable-length and byte size alone is
	// a poor proxy for average chain length. Recomputed authoritatively by
	// rehash's counting pass, so it self-corrects after a resize() truncate.
	private entriesCount = 0;

	private bucketsMap!: MappedFile;
	private bucketsCount = 0;

	private metaMap!: MappedFile;
	private stale = true;

	private constructor(options: HashMapStoreOptions<Pointer, Key, Value>) {
		super();
		this.path = options.path;
		this.pointer = options.pointer;
		this.key = options.key;
		this.value = options.value;
		this.targetLoadFactor = options.loadFactor.target;
		this.maxLoadFactorDrift = options.loadFactor.maxDrift;
		this.growthHeadroom = options.growth.headroom;
		this.growthAmount = options.growth.amount;
		this.entriesPath = join(options.path, "entries");
		this.bucketsPath = join(options.path, "buckets");
		this.metaPath = join(options.path, "meta");
		this.metaSize = 1 + this.pointer.stride.size * 2; // Bool + Pointer(entriesSize) + Pointer(entriesCount)

		this.item = new StructCodec({
			previous: options.pointer,
			key: options.key,
			value: options.value,
		});
		this.header = new StructCodec({
			previous: options.pointer,
			key: options.key,
		});
		this.entry = new TupleCodec([options.key, options.value]);
		this.meta = new StructCodec({
			stale: Bool,
			entriesSize: options.pointer,
			entriesCount: options.pointer,
		});
	}

	static open<Pointer extends FixedCodec<number>, Key extends Codec, Value extends Codec>(
		options: HashMapStoreOptions<Pointer, Key, Value>,
	): HashMapStore<Pointer, Key, Value> {
		if (options.pointer.stride.kind !== "fixed") {
			throw new Error("HashMapStore pointer codec must be fixed-stride");
		}
		if (options.loadFactor.target <= 0) throw new Error("loadFactor.target must be > 0");
		if (options.loadFactor.maxDrift < 0) throw new Error("loadFactor.maxDrift must be >= 0");
		if (!Number.isInteger(options.growth.headroom) || options.growth.headroom < 0) {
			throw new Error("growth.headroom must be an integer >= 0");
		}
		if (!Number.isInteger(options.growth.amount) || options.growth.amount <= 0) {
			throw new Error("growth.amount must be an integer > 0");
		}

		Deno.mkdirSync(options.path, { recursive: true });

		const self = new HashMapStore<Pointer, Key, Value>(options);

		const existsNonEmpty = (p: string): boolean => {
			try {
				return Deno.statSync(p).size > 0;
			} catch (e) {
				if (e instanceof Deno.errors.NotFound) return false;
				throw e;
			}
		};

		// Meta
		let metaExists = true;
		try {
			const stat = Deno.statSync(self.metaPath);
			if (stat.size !== self.metaSize) {
				throw new Error(
					`HashMapStore: meta size mismatch (expected ${self.metaSize}, got ${stat.size}) — wrong pointer codec or corrupted store`,
				);
			}
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
			metaExists = false;
		}
		if (!metaExists) {
			// Refuse to reinitialize over orphaned data files (wrong path or a
			// deleted meta) — a fresh meta would hide them behind zeroed sizes.
			if (existsNonEmpty(self.entriesPath) || existsNonEmpty(self.bucketsPath)) {
				throw new Error("HashMapStore: meta file is missing but data files exist — refusing to reinitialize over them");
			}
			Deno.openSync(self.metaPath, { create: true, write: true }).close();
			Deno.truncateSync(self.metaPath, self.metaSize);
		}
		self.metaMap = mapFile(self.metaPath);
		if (metaExists) {
			const [meta] = self.meta.decode(self.metaMap.bytes);
			self.stale = meta.stale;
			self.entriesSize = meta.entriesSize;
			self.entriesCount = meta.entriesCount;
		}

		// Entries — always created, grown by the normal rule, then mapped. A
		// brand-new file has 0 bytes (0 free < headroom), so creation itself
		// triggers a grow; an existing file whose free space is below headroom
		// is topped up the same way. Growth happens BEFORE the first map —
		// mmapping a 0-byte file is invalid.
		try {
			self.entriesPhysicalSize = Deno.statSync(self.entriesPath).size;
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
			Deno.openSync(self.entriesPath, { create: true, write: true }).close();
			self.entriesPhysicalSize = 0;
		}
		if (self.entriesSize > self.entriesPhysicalSize) {
			throw new Error(
				`HashMapStore: meta entriesSize (${self.entriesSize}) exceeds entries file size (${self.entriesPhysicalSize}) — corrupted store`,
			);
		}
		const newSize = self.grownSize(Math.max(1, self.entriesSize));
		if (newSize !== self.entriesPhysicalSize) {
			Deno.truncateSync(self.entriesPath, newSize);
			self.entriesPhysicalSize = newSize;
		}
		self.entriesMap = mapFile(self.entriesPath);

		if (!metaExists) {
			// Brand-new store: create the buckets file at the configured initial
			// count and persist a clean meta.
			self.stale = false;
			self.entriesSize = 0;
			self.entriesCount = 0;
			self.writeMeta();
			self.metaMap.mapping.flush();
			return self;
		}

		// Buckets (only if not stale — stale means rehash will rebuild them)
		if (!self.stale) {
			try {
				self.bucketsCount = Deno.statSync(self.bucketsPath).size / self.pointer.stride.size;
				if (self.bucketsCount > 0) {
					self.bucketsMap = mapFile(self.bucketsPath);
				}
			} catch (e) {
				if (!(e instanceof Deno.errors.NotFound)) throw e;
			}
		}

		if (self.stale || self.bucketsCount === 0) {
			// Stale: re-run the interrupted rehash. No buckets (but healthy meta):
			// buckets are derived data — rebuild them from the log.
			self.rehash();
		} else {
			// Healthy open: recover any puts that committed their head update
			// but crashed before meta was flushed.
			self.recoverTail();
		}

		return self;
	}

	// ── meta ───────────────────────────────────────────────────────────────

	private writeMeta(): void {
		this.metaMap.bytes.set(this.meta.encode({ stale: this.stale, entriesSize: this.entriesSize, entriesCount: this.entriesCount }));
	}

	// ── entries file (pre-grown, mmap'd) ───────────────────────────────────

	/**
	 * Physical size needed to hold `afterSize` logical bytes under the growth
	 * policy: grow by `growth.amount` whenever free space (physical −
	 * `afterSize`) would drop below `growth.headroom`. Pure sizing — no I/O.
	 */
	private grownSize(afterSize: number): number {
		let size = this.entriesPhysicalSize;
		while (size - afterSize < this.growthHeadroom) size += this.growthAmount;
		return size;
	}

	/** Grow the entries file (per `grownSize`) so it holds `afterSize` logical bytes plus headroom. No-op when the policy is already satisfied. */
	private ensureEntriesCapacity(afterSize: number): void {
		const newSize = this.grownSize(afterSize);
		if (newSize === this.entriesPhysicalSize) return;
		this.entriesMap.mapping.close();
		const file = Deno.openSync(this.entriesPath, { write: true });
		file.truncateSync(newSize);
		file.close();
		this.entriesMap = mapFile(this.entriesPath);
		this.entriesPhysicalSize = newSize;
	}

	/** Byte size of the whole entry (`previous ++ key ++ value`) at `offset`. */
	private entrySizeAt(offset: number): number {
		const [, prefixSize] = this.header.decode(this.entriesMap.bytes, offset);
		const [, valueSize] = this.value.decode(this.entriesMap.bytes, offset + prefixSize);
		return prefixSize + valueSize;
	}

	// ── buckets file (fixed-width array, mmap'd) ───────────────────────────

	private resizeBuckets(count: number): void {
		// bucketsCount > 0 ⟺ bucketsMap is assigned (both are set together in
		// open()/here) — so this guards the very first call from open().
		if (this.bucketsCount > 0) this.bucketsMap.mapping.close();
		// Invariant: at least 1 bucket, so bucketIndexOf never mods by zero.
		this.bucketsCount = count = Math.max(1, count);
		// Delete + recreate (not truncate-in-place): rehash relies on every
		// slot starting at 0 (empty). Truncating an EXISTING file only zero-
		// fills the newly-grown tail, leaving stale non-zero pointers from the
		// previous (differently-sized) layout sitting at the overlapping
		// offsets — those get misread as real heads under the new bucket
		// count, corrupting chains. A fresh file is zero end-to-end.
		try {
			Deno.removeSync(this.bucketsPath);
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
		}
		const file = Deno.openSync(this.bucketsPath, { create: true, write: true });
		file.truncateSync(count * this.pointer.stride.size);
		file.close();
		this.bucketsMap = mapFile(this.bucketsPath);
	}

	private readBucket(index: number): number {
		const [value] = this.pointer.decode(this.bucketsMap.bytes, index * this.pointer.stride.size);
		return value;
	}

	private writeBucket(index: number, value: number): void {
		this.bucketsMap.bytes.set(this.pointer.encode(value), index * this.pointer.stride.size);
	}

	// ── hashing ────────────────────────────────────────────────────────────

	/** FNV-1a (32-bit) over the encoded key bytes. */
	private hashKeyBytes(keyBytes: Uint8Array): number {
		let hash = 0x811c9dc5;
		for (let i = 0; i < keyBytes.length; i++) {
			hash ^= keyBytes[i]!;
			hash = Math.imul(hash, 0x01000193);
		}
		return hash >>> 0;
	}

	private bucketIndexOf(keyBytes: Uint8Array): number {
		return this.hashKeyBytes(keyBytes) % this.bucketsCount;
	}

	// ── read path ──────────────────────────────────────────────────────────

	/**
	 * Find the entry for an encoded key. Compares the query bytes against the
	 * raw key bytes stored in the log (key length = prefixSize - pointerSize)
	 * — the query key is encoded once by the caller, stored keys are never
	 * decoded-then-re-encoded. Returns the 0-based offset and header size.
	 */
	private findEntry(keyBytes: Uint8Array): { offset: number; prefixSize: number } | undefined {
		if (this.bucketsCount === 0) return undefined;
		const pointerSize = this.pointer.stride.size;
		let pointer = this.readBucket(this.bucketIndexOf(keyBytes));
		while (pointer !== 0) {
			const offset = pointer - 1;
			const [prefix, prefixSize] = this.header.decode(this.entriesMap.bytes, offset);
			if (
				keyBytes.length === prefixSize - pointerSize &&
				equals(keyBytes, this.entriesMap.bytes.subarray(offset + pointerSize, offset + prefixSize))
			) {
				return { offset, prefixSize };
			}
			pointer = prefix.previous;
		}
		return undefined;
	}

	get(key: Codec.InferInput<Key>): Codec.InferOutput<Value> | undefined {
		const found = this.findEntry(this.key.encode(key));
		if (!found) return undefined;
		const [value] = this.value.decode(this.entriesMap.bytes, found.offset + found.prefixSize);
		return value;
	}

	getPointer(key: Codec.InferInput<Key>): number | undefined {
		return this.findEntry(this.key.encode(key))?.offset;
	}

	getValueAndPointer(key: Codec.InferInput<Key>): [value: Codec.InferOutput<Value>, pointer: number] | undefined {
		const found = this.findEntry(this.key.encode(key));
		if (!found) return undefined;
		const [value] = this.value.decode(this.entriesMap.bytes, found.offset + found.prefixSize);
		return [value, found.offset];
	}

	getEntry(pointer: number): [TupleOutput<[key: Key, value: Value]>, offset: number] {
		return this.entry.decode(this.entriesMap.bytes, pointer + this.pointer.stride.size);
	}

	getKey(pointer: number): [Codec.InferOutput<Key>, offset: number] {
		return this.key.decode(this.entriesMap.bytes, pointer + this.pointer.stride.size);
	}

	has(key: Codec.InferInput<Key>): boolean {
		return this.findEntry(this.key.encode(key)) !== undefined;
	}

	// ── write path ─────────────────────────────────────────────────────────

	/** Insert `key -> value`. Rejects duplicates; returns the 0-based pointer. */
	put(key: Codec.InferInput<Key>, value: Codec.InferInput<Value>): number {
		const keyBytes = this.key.encode(key);
		const bucket = this.bucketIndexOf(keyBytes);
		const head = this.readBucket(bucket);

		// Reject duplicates: walk the chain, comparing raw key bytes.
		const pointerSize = this.pointer.stride.size;
		let pointer = head;
		while (pointer !== 0) {
			const offset = pointer - 1;
			const [prefix, prefixSize] = this.header.decode(this.entriesMap.bytes, offset);
			if (
				keyBytes.length === prefixSize - pointerSize &&
				equals(keyBytes, this.entriesMap.bytes.subarray(offset + pointerSize, offset + prefixSize))
			) {
				throw new Error("HashMapStore.put: duplicate key");
			}
			pointer = prefix.previous;
		}

		// Append entry, linked back to the old head.
		const offset = this.entriesSize;
		const encoded = this.item.encode({ previous: head, key, value });
		this.ensureEntriesCapacity(offset + encoded.length);
		this.entriesMap.bytes.set(encoded, offset);

		// Order matters for crash safety (see class doc): head FIRST, then
		// meta. Crash before the head: the bytes above are unreachable and the
		// next put overwrites them. Crash after the head but before meta
		// flushes: recoverTail() on open extends entriesSize/entriesCount over
		// the committed tail.
		this.writeBucket(bucket, offset + 1);
		this.entriesSize += encoded.length;
		this.entriesCount += 1;
		this.writeMeta();

		this.maybeRehash();

		return offset;
	}

	/**
	 * Update the value of an existing key, in place. Returns `false` if the
	 * key is absent — never inserts.
	 */
	set(key: Codec.InferInput<Key>, value: Codec.InferInput<Value>): boolean {
		const pointer = this.getPointer(key);
		if (pointer === undefined) return false;
		this.setValue(pointer, value);
		return true;
	}

	/**
	 * Overwrite the value of the entry at `pointer` (a 0-based offset, as
	 * returned by `put` / `getPointer`) in place. The encoded value must be
	 * exactly the same byte length as the existing one — entries are fixed
	 * slots in an append-only log, so a different length can't be spliced in.
	 */
	setValue(pointer: number, value: Codec.InferInput<Value>): void {
		if (pointer < 0 || pointer >= this.entriesSize) {
			throw new Error(`HashMapStore.setValue: pointer ${pointer} out of range [0, ${this.entriesSize})`);
		}
		const [, prefixSize] = this.header.decode(this.entriesMap.bytes, pointer);
		const valueOffset = pointer + prefixSize;
		const [, oldSize] = this.value.decode(this.entriesMap.bytes, valueOffset);
		const encoded = this.value.encode(value);
		if (encoded.length !== oldSize) {
			throw new Error(
				`HashMapStore.setValue: encoded length changed (${oldSize} -> ${encoded.length}) — in-place update requires equal length`,
			);
		}
		this.entriesMap.bytes.set(encoded, valueOffset);
	}

	/**
	 * Write an encoded value at absolute offset `pointer` WITHOUT updating
	 * `entriesSize`, meta, or buckets — staging bytes ahead of the logical end
	 * of the log. `pointer` must be at or past `entriesSize`; for in-place
	 * updates of committed entries use `setValue`. Staged bytes become live
	 * only when `resize()` grows `entriesSize` to cover them (which validates
	 * them as whole entries and rehashes everything, bulk-indexing them).
	 * Until then, the next `put` appends at `entriesSize` and overwrites them.
	 */
	putValue(pointer: number, value: Codec.InferInput<Value>): void {
		if (pointer < this.entriesSize) {
			throw new Error(
				`HashMapStore.putValue: pointer ${pointer} is inside the committed log [0, ${this.entriesSize}) — use setValue for in-place updates`,
			);
		}
		const encoded = this.value.encode(value);
		this.ensureEntriesCapacity(pointer + encoded.length);
		this.entriesMap.bytes.set(encoded, pointer);
	}

	// ── crash recovery ─────────────────────────────────────────────────────

	/**
	 * Recover from `put`'s "head updated, meta not yet flushed" crash window:
	 * bucket heads may point at committed entries past `entriesSize`. Only the
	 * newest entry of each bucket can be uncommitted, so first check just the
	 * heads; on mismatch, walk every chain to extend `entriesSize` /
	 * `entriesCount` over the committed tail.
	 */
	private recoverTail(): void {
		let needsRepair = false;
		for (let i = 0; i < this.bucketsCount; i++) {
			const head = this.readBucket(i);
			if (head !== 0 && head - 1 + this.entrySizeAt(head - 1) > this.entriesSize) {
				needsRepair = true;
				break;
			}
		}
		if (!needsRepair) return;

		let maxEnd = this.entriesSize;
		let extra = 0;
		for (let i = 0; i < this.bucketsCount; i++) {
			let pointer = this.readBucket(i);
			while (pointer !== 0) {
				const offset = pointer - 1;
				const [prefix] = this.header.decode(this.entriesMap.bytes, offset);
				const end = offset + this.entrySizeAt(offset);
				if (offset >= this.entriesSize) extra++;
				if (end > maxEnd) maxEnd = end;
				pointer = prefix.previous;
			}
		}
		this.entriesSize = maxEnd;
		this.entriesCount += extra;
		this.writeMeta();
	}

	// ── rehash ─────────────────────────────────────────────────────────────

	private maybeRehash(): void {
		if (this.entriesCount === 0) return;
		// Standard load factor: entries per bucket (NOT buckets per entry).
		const loadFactor = this.entriesCount / this.bucketsCount;
		const low = this.targetLoadFactor * (1 - this.maxLoadFactorDrift);
		const high = this.targetLoadFactor * (1 + this.maxLoadFactorDrift);
		if (loadFactor < low || loadFactor > high) this.rehash();
	}

	/**
	 * Rebuild the whole hash structure against the entries log. Replays entries
	 * oldest-first, patching each entry's inline `previous` and rebuilding heads.
	 * Crash-safe via the `stale` flag in meta.
	 *
	 * Also recomputes `entriesCount` from scratch (a separate counting pass
	 * before the bucket rebuild) rather than trusting the incrementally-
	 * tracked field — shrinking `resize()` truncates the log by byte offset
	 * alone and has no cheap way to know how many entries that corresponds to,
	 * so this is the one place that count is allowed to go stale and gets
	 * corrected.
	 */
	rehash(): void {
		// (0) persist the current committed sizes BEFORE marking stale: if we
		// crash mid-rehash, the recovery rescan must cover every entry that was
		// reachable when we started (meta on disk could otherwise lag the heads).
		this.writeMeta();
		this.metaMap.mapping.flush();

		// (1) mark stale + flush
		this.stale = true;
		this.writeMeta();
		this.metaMap.mapping.flush();

		const total = this.entriesSize;

		// (2) count entries (authoritative)
		let count = 0;
		for (let offset = 0; offset < total;) {
			offset += this.entrySizeAt(offset);
			count++;
		}
		this.entriesCount = count;

		// (3) reset buckets, sized off entry count (not byte size). Load factor
		// is entries/buckets, so buckets = entries/targetLoadFactor.
		const bucketCount = Math.max(1, Math.round(this.entriesCount / this.targetLoadFactor));
		this.resizeBuckets(bucketCount);

		// (4) replay entries oldest-first, patching links + rebuilding heads.
		// Hash the raw key bytes straight out of the log — no decode/re-encode.
		const pointerSize = this.pointer.stride.size;
		let offset = 0;
		while (offset < total) {
			const [prefix, prefixSize] = this.header.decode(this.entriesMap.bytes, offset);
			const [, valueSize] = this.value.decode(this.entriesMap.bytes, offset + prefixSize);
			const keyBytes = this.entriesMap.bytes.subarray(offset + pointerSize, offset + prefixSize);

			const bucket = this.bucketIndexOf(keyBytes);
			const head = this.readBucket(bucket);

			if (prefix.previous !== head) {
				this.entriesMap.bytes.set(this.pointer.encode(head), offset);
			}
			this.writeBucket(bucket, offset + 1);

			offset += prefixSize + valueSize;
		}

		// (5) clear stale + flush
		this.stale = false;
		this.writeMeta();
		this.metaMap.mapping.flush();
	}

	// ── Store contract ─────────────────────────────────────────────────────

	/** Size == byte length of the entries log. */
	override size(): number {
		return this.entriesSize;
	}

	/**
	 * Shrink: truncate the logical view to `size` bytes and re-point any bucket
	 * heads left dangling into the truncated tail (backwards `previous` links
	 * keep surviving chains intact, so no rehash is needed). `size` must be an
	 * entry boundary (e.g. an earlier `size()`); a mid-entry cut throws before
	 * anything is modified. `entriesCount` goes stale here by design — the next
	 * rehash recounts.
	 *
	 * Grow: cover bytes staged ahead of `entriesSize` (see `putValue`). The new
	 * region must decode as whole entries landing exactly on `size`; then a
	 * full rehash hashes and indexes them in bulk.
	 */
	override resize(size: number): void {
		if (!Number.isInteger(size) || size < 0) {
			throw new Error(`HashMapStore.resize: invalid size ${size}`);
		}
		if (size === this.entriesSize) return;

		if (size < this.entriesSize) {
			// Compute + validate all new heads FIRST (a mid-entry cut must not
			// leave half the buckets re-pointed), then commit.
			const newHeads = new Array<number>(this.bucketsCount);
			for (let i = 0; i < this.bucketsCount; i++) {
				let pointer = this.readBucket(i);
				while (pointer !== 0 && pointer - 1 >= size) {
					const [prefix] = this.header.decode(this.entriesMap.bytes, pointer - 1);
					pointer = prefix.previous;
				}
				if (pointer !== 0 && pointer - 1 + this.entrySizeAt(pointer - 1) > size) {
					throw new Error(`HashMapStore.resize: ${size} cuts through an entry — size must be an entry boundary`);
				}
				newHeads[i] = pointer;
			}
			for (let i = 0; i < this.bucketsCount; i++) {
				this.writeBucket(i, newHeads[i]!);
			}
			this.entriesSize = size;
			this.writeMeta();
			return;
		}

		// Grow over staged bytes: validate they decode as whole entries landing
		// exactly on `size` BEFORE committing the new size.
		if (size > this.entriesPhysicalSize) {
			throw new Error(
				`HashMapStore.resize: ${size} exceeds physical capacity ${this.entriesPhysicalSize} — nothing staged there`,
			);
		}
		try {
			let offset = this.entriesSize;
			while (offset < size) {
				offset += this.entrySizeAt(offset);
			}
			if (offset !== size) {
				throw new Error(`staged entries overran the target (ended at ${offset})`);
			}
		} catch (e) {
			throw new Error(
				`HashMapStore.resize: staged region [${this.entriesSize}, ${size}) doesn't decode as whole entries`,
				{ cause: e },
			);
		}
		this.entriesSize = size;
		this.rehash(); // recount + rebuild, indexing the staged entries in bulk
	}

	override sync(): void {
		this.writeMeta();
		this.metaMap.mapping.flush();
		this.entriesMap.mapping.flush();
		this.bucketsMap.mapping.flush();
	}

	close(): void {
		this.sync();
		this.entriesMap.mapping.close();
		this.bucketsMap.mapping.close();
		this.metaMap.mapping.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
