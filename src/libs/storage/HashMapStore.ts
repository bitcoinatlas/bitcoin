import { Bool, Codec, FixedCodec, StructCodec } from "@nomadshiba/codec";
import { equals } from "@std/bytes/equals";
import { Mmap } from "@nomadshiba/mmap";
import { join } from "@std/path";
import { Store } from "~/libs/storage/Store.ts";

export type LoadFactorOptions = {
	/** Target average entries per bucket (`entryCount / bucketCount`) — same meaning as e.g. Java `HashMap`'s load factor. `1` aims for ~1 entry/bucket, `4` aims for ~4 (fewer, cheaper buckets; longer chains). Must be > 0. */
	target: number;
	/** How far the live load factor may drift from `target` before a rehash. Must be >= 0. */
	maxDrift: number;
};

export type HashMapStoreOptions<Pointer extends FixedCodec<number>, Key extends Codec, Value extends Codec> = {
	path: string;
	pointer: Pointer;
	key: Key;
	value: Value;
	loadFactor: LoadFactorOptions;
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
 *   Pre-grown past its logical size; the logical size is tracked in `meta`.
 * - `buckets` (`path/buckets`): flat array of fixed-width pointer slots.
 *   `buckets[hash(key) % count]` = head (newest entry) of that bucket.
 * - `meta` (`path/meta`): `{ stale, entriesSize, entriesCount }`. `stale` is
 *   set during rehash for crash safety; `entriesSize` is the entries file's
 *   logical size (bytes); `entriesCount` is the number of entries, kept for
 *   the load-factor math (see `loadFactor`) since entries are variable-length
 *   and byte size alone is a poor proxy for chain length.
 *
 * ## Null sentinel
 * Persisted pointers are stored +1, so 0 = empty slot / end of chain.
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
 * — it's the one place that field is allowed to go stale (`resize()` truncates
 * by byte offset alone) and gets corrected. Crash-safe via the `stale` flag —
 * a crash mid-rehash is detected on `open` and re-run.
 *
 * ## Crash safety of `set`
 * An entry append + head update is not atomic across a crash. If we crash
 * after the entry lands but before the head is updated, the entry is orphaned
 * (present but unreachable) — a subsequent rehash cleans it up. If we crash
 * after both, the meta's `entriesSize`/`entriesCount` may lag (written via
 * mmap, flushed on `close`/`rehash`); on reopen the orphaned tail is simply
 * not counted.
 */
export class HashMapStore<Pointer extends FixedCodec<number>, Key extends Codec, Value extends Codec> extends Store implements Disposable {
	public readonly path: string;

	private readonly Pointer: Pointer;
	private readonly key: Key;
	private readonly value: Value;
	private readonly entry: StructCodec<{ previous: Pointer; key: Key; value: Value }>;
	private readonly entryPrefix: StructCodec<{ previous: Pointer; key: Key }>;
	private readonly meta: StructCodec<{ stale: typeof Bool; entriesSize: Pointer; entriesCount: Pointer }>;
	private readonly targetLoadFactor: number;
	private readonly maxLoadFactorDrift: number;

	private readonly entriesPath: string;
	private readonly bucketsPath: string;
	private readonly metaPath: string;
	private readonly pointerSize: number;
	private readonly metaSize: number;

	private entriesMap: Mmap | undefined;
	private entriesPhysicalSize = 0;
	private entriesSize = 0;
	// Number of entries (not bytes) — used for the bucket-ratio math instead
	// of entriesSize, since entries are variable-length and byte size alone is
	// a poor proxy for average chain length. Recomputed authoritatively by
	// rehash's counting pass, so it self-corrects after a resize() truncate.
	private entriesCount = 0;

	private bucketsMap: Mmap | undefined;
	private bucketsCount = 0;

	private metaMap: Mmap | undefined;
	private stale = true;

	// A fresh key per (re)map of entries/buckets opts each one out of Mmap's
	// default same-path sharing cache — every resize is a mapping we own
	// exclusively and fully close before the next one opens.
	private mmapGeneration = 0;

	private constructor(options: HashMapStoreOptions<Pointer, Key, Value>) {
		super();
		this.path = options.path;
		this.Pointer = options.pointer;
		this.key = options.key;
		this.value = options.value;
		this.targetLoadFactor = options.loadFactor.target;
		this.maxLoadFactorDrift = options.loadFactor.maxDrift;
		this.entriesPath = join(options.path, "entries");
		this.bucketsPath = join(options.path, "buckets");
		this.metaPath = join(options.path, "meta");
		this.pointerSize = options.pointer.stride.size;
		this.metaSize = 1 + this.pointerSize * 2; // Bool + Pointer(entriesSize) + Pointer(entriesCount)

		this.entry = new StructCodec({
			previous: options.pointer,
			key: options.key,
			value: options.value,
		});
		this.entryPrefix = new StructCodec({
			previous: options.pointer,
			key: options.key,
		});
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

		Deno.mkdirSync(options.path, { recursive: true });

		const self = new HashMapStore<Pointer, Key, Value>(options);

		// Meta
		let metaExists = true;
		try {
			const stat = Deno.statSync(self.metaPath);
			if (stat.size !== self.metaSize) {
				metaExists = false;
			}
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
			metaExists = false;
		}
		if (!metaExists) {
			Deno.openSync(self.metaPath, { create: true, write: true }).close();
			Deno.truncateSync(self.metaPath, self.metaSize);
		}
		self.metaMap = Mmap.openSync(self.metaPath, { write: true });
		if (metaExists) {
			const [meta] = self.meta.decode(self.metaMap.bytes);
			self.stale = meta.stale;
			self.entriesSize = meta.entriesSize;
			self.entriesCount = meta.entriesCount;
		}

		// Entries
		try {
			self.entriesPhysicalSize = Deno.statSync(self.entriesPath).size;
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
			Deno.openSync(self.entriesPath, { create: true, write: true }).close();
			self.entriesPhysicalSize = 0;
		}
		if (self.entriesPhysicalSize > 0) {
			self.entriesMap = Mmap.openSync(self.entriesPath, { write: true, key: `${self.mmapGeneration++}` });
		}

		// Buckets (only if not stale — stale means rehash will create them)
		if (!self.stale) {
			try {
				self.bucketsCount = Deno.statSync(self.bucketsPath).size / self.pointerSize;
				if (self.bucketsCount > 0) {
					self.bucketsMap = Mmap.openSync(self.bucketsPath, { write: true, key: `${self.mmapGeneration++}` });
				}
			} catch (e) {
				if (!(e instanceof Deno.errors.NotFound)) throw e;
			}
		}

		if (self.stale) self.rehash();

		return self;
	}

	// ── meta ───────────────────────────────────────────────────────────────

	private writeMeta(): void {
		this.metaMap!.bytes.set(this.meta.encode({ stale: this.stale, entriesSize: this.entriesSize, entriesCount: this.entriesCount }));
	}

	// ── entries file (pre-grown, mmap'd) ───────────────────────────────────

	private ensureEntriesCapacity(needed: number): void {
		if (needed <= this.entriesPhysicalSize) return;
		let newSize = this.entriesPhysicalSize;
		if (newSize === 0) newSize = 1024 * 1024;
		while (newSize < needed) newSize *= 2;
		this.entriesMap?.close();
		this.entriesMap = undefined;
		const file = Deno.openSync(this.entriesPath, { write: true });
		file.truncateSync(newSize);
		file.close();
		this.entriesMap = Mmap.openSync(this.entriesPath, { write: true, key: `${this.mmapGeneration++}` });
		this.entriesPhysicalSize = newSize;
	}

	// ── buckets file (fixed-width array, mmap'd) ───────────────────────────

	private resizeBuckets(count: number): void {
		this.bucketsMap?.close();
		this.bucketsMap = undefined;
		this.bucketsCount = count;
		if (count === 0) return;
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
		file.truncateSync(count * this.pointerSize);
		file.close();
		this.bucketsMap = Mmap.openSync(this.bucketsPath, { write: true, key: `${this.mmapGeneration++}` });
	}

	private readBucket(index: number): number {
		const [value] = this.Pointer.decode(this.bucketsMap!.bytes, index * this.pointerSize);
		return value;
	}

	private writeBucket(index: number, value: number): void {
		this.bucketsMap!.bytes.set(this.Pointer.encode(value), index * this.pointerSize);
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

	get(key: Codec.InferInput<Key>): Codec.InferOutput<Value> | undefined {
		if (this.bucketsCount === 0) return undefined;
		const keyBytes = this.key.encode(key);
		let pointer = this.readBucket(this.bucketIndexOf(keyBytes));
		while (pointer !== 0) {
			pointer -= 1;
			const [prefix, prefixSize] = this.entryPrefix.decode(this.entriesMap!.bytes, pointer);
			if (equals(keyBytes, this.key.encode(prefix.key))) {
				const [value] = this.value.decode(this.entriesMap!.bytes, pointer + prefixSize);
				return value;
			}
			pointer = prefix.previous;
		}
		return undefined;
	}

	async getAsync(key: Codec.InferInput<Key>): Promise<Codec.InferOutput<Value> | undefined> {
		return this.get(key);
	}

	has(key: Codec.InferInput<Key>): boolean {
		return this.get(key) !== undefined;
	}

	// ── write path ─────────────────────────────────────────────────────────

	/** Insert `key -> value`. Rejects duplicates; returns `true` on fresh insert. */
	set(key: Codec.InferInput<Key>, value: Codec.InferInput<Value>): boolean {
		const keyBytes = this.key.encode(key);
		const bucket = this.bucketIndexOf(keyBytes);
		const head = this.readBucket(bucket);

		// Reject duplicates: walk the chain.
		let pointer = head;
		while (pointer !== 0) {
			pointer -= 1;
			const [prefix] = this.entryPrefix.decode(this.entriesMap!.bytes, pointer);
			if (equals(keyBytes, this.key.encode(prefix.key))) return false;
			pointer = prefix.previous;
		}

		// Append entry, link back to old head, make it the new head (+1 encoded).
		const offset = this.entriesSize;
		const encoded = this.entry.encode({ previous: head, key, value });
		this.ensureEntriesCapacity(offset + encoded.length);
		this.entriesMap!.bytes.set(encoded, offset);
		this.entriesSize += encoded.length;
		this.entriesCount += 1;
		this.writeMeta();

		this.writeBucket(bucket, offset + 1);

		this.maybeRehash();
		return true;
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
	 * tracked field — `resize()` truncates the log by byte offset alone and
	 * has no cheap way to know how many entries that corresponds to, so this
	 * is the one place that count is allowed to go stale and gets corrected.
	 */
	rehash(): void {
		// (1) mark stale + flush
		this.stale = true;
		this.writeMeta();
		this.metaMap!.flush();

		const total = this.entriesSize;

		// (2) count entries (authoritative)
		let count = 0;
		for (let offset = 0; offset < total;) {
			const [, prefixSize] = this.entryPrefix.decode(this.entriesMap!.bytes, offset);
			const [, valueSize] = this.value.decode(this.entriesMap!.bytes, offset + prefixSize);
			offset += prefixSize + valueSize;
			count++;
		}
		this.entriesCount = count;

		// (3) reset buckets, sized off entry count (not byte size). Load factor
		// is entries/buckets, so buckets = entries/targetLoadFactor.
		const bucketCount = Math.max(1, Math.round(this.entriesCount / this.targetLoadFactor));
		this.resizeBuckets(bucketCount);

		// (4) replay entries oldest-first, patching links + rebuilding heads
		let offset = 0;
		while (offset < total) {
			const [prefix, prefixSize] = this.entryPrefix.decode(this.entriesMap!.bytes, offset);
			const [, valueSize] = this.value.decode(this.entriesMap!.bytes, offset + prefixSize);
			const keyBytes = this.key.encode(prefix.key);

			const bucket = this.bucketIndexOf(keyBytes);
			const head = this.readBucket(bucket);

			if (prefix.previous !== head) {
				this.entriesMap!.bytes.set(this.Pointer.encode(head), offset);
			}
			this.writeBucket(bucket, offset + 1);

			offset += prefixSize + valueSize;
		}

		// (5) clear stale + flush
		this.stale = false;
		this.writeMeta();
		this.metaMap!.flush();
	}

	// ── Store contract ─────────────────────────────────────────────────────

	/** Size == byte length of the entries log. */
	override size(): number {
		return this.entriesSize;
	}

	/** Truncate the entries log to `size` bytes, then rebuild heads via rehash. */
	override resize(size: number): void {
		this.entriesSize = size;
		this.rehash();
	}

	close(): void {
		this.writeMeta();
		this.metaMap?.flush();
		this.entriesMap?.flush();
		this.bucketsMap?.flush();
		this.entriesMap?.close();
		this.bucketsMap?.close();
		this.metaMap?.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
