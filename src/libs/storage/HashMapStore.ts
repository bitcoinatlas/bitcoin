import { Bool, Codec, FixedCodec, StructCodec } from "@nomadshiba/codec";
import { equals } from "@std/bytes/equals";
import { join } from "@std/path";
import { ArrayStore, ArrayStoreOptions } from "~/libs/storage/ArrayStore.ts";
import { BlobStore, BlobStoreOptions } from "~/libs/storage/BlobStore.ts";
import { Store } from "~/libs/storage/Store.ts";

export type HashMapStoreOptions<Pointer extends FixedCodec<number>, Key extends Codec, Value extends Codec> = {
	path: string;
	pointer: Pointer;
	key: Key;
	value: Value;
	entries: Pick<BlobStoreOptions, "compression" | "maxChunkSize">;
	buckets: Pick<ArrayStoreOptions<Pointer>, "compression" | "itemsPerChunk">;
	/** Ideal buckets/entries ratio; the map keeps `buckets ≈ entries * targetRatio`. Must be > 0. */
	targetRatio: number;
	/** How far the live buckets/entries ratio may drift from `targetRatio` before a rehash. Must be >= 0. */
	maxRatioDrift: number;
	writable: boolean;
};

type Meta = Codec.InferOutput<typeof Meta>;
const Meta = new StructCodec({ stale: Bool });

/**
 * On-disk hash map layered on top of {@link BlobStore} (entries) and
 * {@link ArrayStore} (bucket heads).
 *
 * ## Layout
 * - `entries` (BlobStore, `path/entries`): append-only log. Each entry is
 *   `previous ++ key ++ value` (a {@link StructCodec}). `previous` is the first
 *   field so the fixed-width link can be read/patched without touching the
 *   variable-length key/value tail. It links backwards to the previous entry
 *   in the same bucket — a per-bucket singly-linked list.
 * - `buckets` (ArrayStore<Pointer>, `path/heads`): `buckets[hash(key) % len]`
 *   is the HEAD — a pointer to the *newest* entry in that bucket. Walking
 *   `HEAD -> previous` visits the whole bucket newest-first.
 * - `meta` (sidecar, `path/meta`): `{ stale }`. Everything else (bucket count,
 *   entry positions) is derived from the two stores.
 *
 * ## Null sentinel
 * Every persisted pointer is stored offset by `+1`, so `0` means "empty slot" /
 * "end of chain". A real entry at byte offset `0` is persisted as `1`.
 *
 * ## Truncate safety
 * `previous` always points backwards to a lower byte offset, so any surviving
 * entry only references entries that also survive a truncate. Truncating the
 * entries log therefore leaves every survivor's chain intact; only the bucket
 * heads need rebuilding, which a rehash does.
 *
 * ## Rehash
 * Changing the bucket count reassigns keys to different buckets, invalidating
 * the inline `previous` links. Rehash replays entries oldest-first, patching
 * each entry's inline `previous` in place and rebuilding the heads.
 *
 * ## Crash safety
 * The heads (and inline links) are a rebuildable projection of the entries log.
 * A rebuild sets `meta.stale = true`, rewrites heads/links, then clears it. A
 * crash mid-rebuild is detected on `open` (via `stale`) and re-runs the rebuild.
 */
export class HashMapStore<Pointer extends FixedCodec<number>, Key extends Codec, Value extends Codec> extends Store implements Disposable {
	public readonly path: string;

	public readonly entries: BlobStore;
	public readonly buckets: ArrayStore<Pointer>;

	private readonly Pointer: Pointer;
	private readonly Key: Key;
	private readonly Value: Value;
	private readonly Entry: StructCodec<{ previous: Pointer; key: Key; value: Value }>;
	private readonly EntryPrefix: StructCodec<{ previous: Pointer; key: Key }>;

	private readonly writable: boolean;
	private readonly targetRatio: number;
	private readonly maxRatioDrift: number;

	private readonly metaPath: string;
	private meta: Meta;

	private constructor(
		entries: BlobStore,
		buckets: ArrayStore<Pointer>,
		meta: Meta,
		options: HashMapStoreOptions<Pointer, Key, Value>,
	) {
		super();
		this.path = options.path;
		this.entries = entries;
		this.buckets = buckets;
		this.meta = meta;
		this.Pointer = options.pointer;
		this.Key = options.key;
		this.Value = options.value;
		this.writable = options.writable;
		this.targetRatio = options.targetRatio;
		this.maxRatioDrift = options.maxRatioDrift;
		this.metaPath = join(options.path, "meta");

		this.Entry = new StructCodec({
			previous: options.pointer,
			key: options.key,
			value: options.value,
		});
		this.EntryPrefix = new StructCodec({
			previous: options.pointer,
			key: options.key,
		});
	}

	static open<Pointer extends FixedCodec<number>, Key extends Codec, Value extends Codec>(
		options: HashMapStoreOptions<Pointer, Key, Value>,
	): HashMapStore<Pointer, Key, Value> {
		if (options.pointer.stride.kind !== "fixed") {
			throw new Error("HashMapStore pointer codec must be fixed-stride");
		}
		if (options.targetRatio <= 0) throw new Error("targetRatio must be > 0");
		if (options.maxRatioDrift < 0) throw new Error("maxRatioDrift must be >= 0");

		const entries = BlobStore.open({
			...options.entries,
			path: join(options.path, "entries"),
			writable: options.writable,
		});

		const buckets = ArrayStore.open({
			...options.buckets,
			path: join(options.path, "heads"),
			item: options.pointer,
			writable: options.writable,
		});

		const metaPath = join(options.path, "meta");
		let meta: Meta | undefined;
		try {
			[meta] = Meta.decode(Deno.readFileSync(metaPath));
		} catch (e) {
			if (!(e instanceof Deno.errors.NotFound)) throw e;
		}
		meta ??= { stale: true };

		const self = new HashMapStore<Pointer, Key, Value>(entries, buckets, meta, options);

		if (!meta.stale) return self;
		if (!options.writable) {
			throw new Error(`HashMapStore at ${options.path} needs a rebuild but was opened read-only`);
		}
		self.rehash();
		return self;
	}

	private writeMeta(): void {
		Deno.writeFileSync(this.metaPath, Meta.encode(this.meta));
	}

	// ── hashing ─────────────────────────────────────────────────────────────

	/** FNV-1a (32-bit) over the encoded key bytes. Codec-agnostic. */
	private hashKeyBytes(keyBytes: Uint8Array): number {
		let hash = 0x811c9dc5;
		for (let i = 0; i < keyBytes.length; i++) {
			hash ^= keyBytes[i]!;
			hash = Math.imul(hash, 0x01000193);
		}
		return hash >>> 0;
	}

	private bucketIndexOf(keyBytes: Uint8Array): number {
		return this.hashKeyBytes(keyBytes) % this.buckets.size();
	}

	// ── read path ─────────────────────────────────────────────────────────────

	get(key: Codec.InferInput<Key>): Codec.InferOutput<Value> | undefined {
		if (this.buckets.size() === 0) return undefined;
		const keyBytes = this.Key.encode(key);
		let pointer = this.buckets.get(this.bucketIndexOf(keyBytes)) ?? 0; // +1 encoded
		while (pointer !== 0) {
			pointer -= 1;
			const [prefix, prefixSize] = this.entries.get(pointer, this.EntryPrefix);
			if (equals(keyBytes, this.Key.encode(prefix.key))) {
				const [value] = this.entries.get(pointer + prefixSize, this.Value);
				return value;
			}
			pointer = prefix.previous;
		}
		return undefined;
	}

	async getAsync(key: Codec.InferInput<Key>): Promise<Codec.InferOutput<Value> | undefined> {
		if (this.buckets.size() === 0) return undefined;
		const keyBytes = this.Key.encode(key);
		let pointer = (await this.buckets.getAsync(this.bucketIndexOf(keyBytes))) ?? 0; // +1 encoded
		while (pointer !== 0) {
			pointer -= 1;
			const [prefix, prefixSize] = await this.entries.getAsync(pointer, this.EntryPrefix);
			if (equals(keyBytes, this.Key.encode(prefix.key))) {
				const [value] = await this.entries.getAsync(pointer + prefixSize, this.Value);
				return value;
			}
			pointer = prefix.previous;
		}
		return undefined;
	}

	has(key: Codec.InferInput<Key>): boolean {
		return this.get(key) !== undefined;
	}

	// ── write path ────────────────────────────────────────────────────────────

	/**
	 * Insert `key -> value`. Rejects duplicates: if `key` already exists the
	 * entry is left untouched and `false` is returned; a fresh insert returns
	 * `true`.
	 */
	set(key: Codec.InferInput<Key>, value: Codec.InferInput<Value>): boolean {
		if (!this.writable) throw new Error("HashMapStore opened read-only");
		const keyBytes = this.Key.encode(key);
		const bucket = this.bucketIndexOf(keyBytes);
		const head = this.buckets.get(bucket) ?? 0; // +1 encoded

		// Reject duplicates: walk the chain and bail if the key is already present.
		let pointer = head;
		while (pointer !== 0) {
			pointer -= 1;
			const [prefix] = this.entries.get(pointer, this.EntryPrefix);
			if (equals(keyBytes, this.Key.encode(prefix.key))) return false;
			pointer = prefix.previous;
		}

		// Append the new entry, linking it back to the (old) bucket head, then
		// make it the new head (+1 encoded).
		const offset = this.entries.append(this.Entry.encode({ previous: head, key, value }));
		this.setHead(bucket, offset + 1);

		this.maybeRehash();
		return true;
	}

	/** Overwrite head slot `bucket` with `head` (already +1 encoded), growing if needed. */
	private setHead(bucket: number, head: number): void {
		const length = this.buckets.size();
		if (bucket < length) {
			this.buckets.set(bucket, head);
			return;
		}
		for (let i = length; i < bucket; i++) this.buckets.push(0);
		this.buckets.push(head);
	}

	// ── rehash ────────────────────────────────────────────────────────────────

	/** Rehash if the live buckets/entries ratio has drifted past `maxRatioDrift`. */
	private maybeRehash(): void {
		const entries = this.entries.size();
		if (entries === 0) return;
		const ratio = this.buckets.size() / entries;
		const low = this.targetRatio * (1 - this.maxRatioDrift);
		const high = this.targetRatio * (1 + this.maxRatioDrift);
		if (ratio < low || ratio > high) this.rehash();
	}

	/**
	 * Rebuild the whole hash structure against the entries log. Replays entries
	 * oldest-first so that, within each bucket, the newest entry ends up as the
	 * head and every inline `previous` points backwards to a lower byte offset.
	 *
	 * Both the bucket heads and the inline `previous` links are rewritten (the
	 * latter in place inside the entries log). Crash-safe via the `stale` flag.
	 */
	rehash(): void {
		if (!this.writable) throw new Error("HashMapStore opened read-only");
		const total = this.entries.size();
		const bucketCount = Math.max(1, Math.round(total * this.targetRatio));

		// (1) mark stale
		this.meta.stale = true;
		this.writeMeta();

		// Reset heads to `bucketCount` empty (0) slots.
		this.buckets.truncate(0);
		for (let i = 0; i < bucketCount; i++) this.buckets.push(0);

		// (2) replay entries oldest-first, repointing each entry at the current
		// head of its (freshly assigned) bucket, then making it the new head.
		let offset = 0;
		while (offset < total) {
			const [prefix, prefixSize] = this.entries.get(offset, this.EntryPrefix);
			const [, valueSize] = this.entries.get(offset + prefixSize, this.Value);
			const keyBytes = this.Key.encode(prefix.key);

			const bucket = this.bucketIndexOf(keyBytes);
			const head = this.buckets.get(bucket) ?? 0;

			// Patch the inline `previous` (fixed-width first field) only if changed.
			if (prefix.previous !== head) {
				this.entries.writeInto(offset, this.Pointer.encode(head));
			}
			this.buckets.set(bucket, offset + 1); // +1 encoded head

			// NOT `offset + prefixSize + valueSize`: append() may have padded a gap
			// between this entry and the next one (sealing a chunk early rather than
			// letting an entry straddle it) — nextPointer skips over that gap.
			offset = this.entries.nextPointer(offset, prefixSize + valueSize);
		}

		// (3) clear stale
		this.meta.stale = false;
		this.writeMeta();
	}

	// ── Store contract ────────────────────────────────────────────────────────

	/** Size == byte length of the entries log (the pin/truncate unit). */
	override size(): number {
		return this.entries.size();
	}

	/**
	 * Truncate the entries log down to `size` bytes, then rebuild the heads via
	 * a rehash. Entries first, heads second: `previous` always points to a lower
	 * offset, so the truncated log is self-consistent before the rehash runs.
	 */
	override truncate(size: number): void {
		if (!this.writable) throw new Error("HashMapStore opened read-only");

		// Mark stale up front so a crash between the entries truncate and the
		// rehash is recovered on the next open.
		this.meta.stale = true;
		this.writeMeta();

		this.entries.truncate(size);
		this.rehash();
	}

	close(): void {
		this.entries.close();
		this.buckets.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
