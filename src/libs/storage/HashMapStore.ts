import { Codec, FixedCodec, StructCodec, U64 } from "@nomadshiba/codec";
import { equals } from "@std/bytes";
import { join } from "@std/path";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { BlobStore } from "~/libs/storage/BlobStore.ts";
import { SharedSlotArray } from "~/libs/storage/SharedSlotArray.ts";
import { Store } from "~/libs/storage/Store.ts";
import { IfNever } from "~/types.ts";

export type LoadFactorOptions = { target: number; maxDrift: number };
export type HashMapStoreOptions<Key extends Codec, Value extends Codec> =
	& {
		commiter: boolean;
		path: string;
		key: Key;
		value: Value;
		loadFactor: LoadFactorOptions;
		entryChunkSize: number;
		minBucketChunkSize: number;
	}
	& IfNever<Extract<Key, FixedCodec> & Extract<Value, FixedCodec>, { maxEntrySize: number }, { maxEntrySize?: undefined }>;

const POINTER_SIZE = SharedSlotArray.BYTES_PER_SLOT;
const INITIAL_BUCKET_COUNT = 1 << 16;

export class HashMapStore<Key extends Codec, Value extends Codec> extends Store implements Disposable {
	public readonly path: string;
	public readonly commiter: boolean;

	public readonly key: Key;
	public readonly value: Value;
	public readonly entry: StructCodec<{ key: Key; value: Value }>;
	private readonly maxEntrySize: number;
	// Every mmap() read of a record needs enough room for [pointer][key][value] —
	// this is the same bound `next()` reserves per record, kept here so every
	// call site asks for it explicitly instead of relying on mmap()'s old
	// "whatever's left in the chunk" default (see BlobStore.mmap).
	private readonly recordMaxSize: number;

	private readonly entries: BlobStore;
	private readonly buckets: SharedSlotArray;
	private readonly counter: SharedSlotArray;
	private readonly loadFactor: LoadFactorOptions;

	// `cursor` is the REVEAL high-water: how far this worker can *see*. It's
	// advanced by every reveal — broadcast (already-persisted regions, findable
	// in the shared buckets) or manual (this worker's own staged entries, held in
	// `newEntries` until pin). `committedCursor` is the PERSIST high-water: how
	// far entries have actually been threaded into the shared bucket index. The
	// two diverge because a committer reveals its staged entries (moving `cursor`)
	// *before* pin writes them into buckets (moving `committedCursor`). `commit`
	// must iterate from `committedCursor`, not `cursor` — keying it off `cursor`
	// made it a no-op whenever reveal had already run, so entries lived only in
	// the in-memory `newEntries` map and never hit disk, leaving buckets empty on
	// the next boot.
	private cursor: number;
	private committedCursor: number;

	private readonly newEntries = new FastUint8ArrayMap<number>();

	private readonly keyScratch_: Uint8Array;

	private constructor(options: HashMapStoreOptions<Key, Value>) {
		super();
		this.path = options.path;
		this.commiter = options.commiter;
		this.key = options.key;
		this.value = options.value;
		this.entry = new StructCodec({ key: options.key, value: options.value });
		this.maxEntrySize = options.maxEntrySize ?? this.entry.stride.size!;
		this.recordMaxSize = POINTER_SIZE + this.maxEntrySize;
		this.keyScratch_ = new Uint8Array(this.maxEntrySize);
		this.loadFactor = options.loadFactor;
		this.cursor = 0;
		this.committedCursor = 0;
		this.entries = BlobStore.open({
			path: join(options.path, "entries"),
			chunkSize: options.entryChunkSize,
		});
		this.buckets = SharedSlotArray.open({
			path: join(options.path, "buckets"),
			writable: options.commiter,
			minChunkSize: options.minBucketChunkSize,
		});
		if (options.commiter && this.buckets.size() === 0) {
			this.buckets.resize(INITIAL_BUCKET_COUNT);
		}
		this.counter = SharedSlotArray.open({
			path: join(options.path, "count"),
			writable: options.commiter,
			minChunkSize: POINTER_SIZE,
		});
		if (options.commiter && this.counter.size() === 0) {
			this.counter.resize(1);
		}
	}

	public static open<Key extends Codec, Value extends Codec>(
		options: HashMapStoreOptions<Key, Value>,
	): HashMapStore<Key, Value> {
		if (options.loadFactor.target <= 0 || options.loadFactor.maxDrift < 0) throw new Error("loadFactor must be positive");
		return new HashMapStore(options);
	}

	public size(): number {
		return this.cursor;
	}

	public next(from?: number): number {
		// Records are laid out `[pointer:POINTER_SIZE][key][value]`, but every
		// offset the store hands out and stores in buckets is the ENTRY offset —
		// the key position, POINTER_SIZE past the record start. Reserve room for a
		// whole record that won't straddle a chunk boundary, then step past the
		// pointer. This also keeps entry offsets >= POINTER_SIZE, so the first
		// entry never lands at 0 (the empty-bucket / end-of-chain sentinel) and
		// `entryOffset - POINTER_SIZE` is always a valid record start.
		return this.entries.next(this.recordMaxSize, from) + POINTER_SIZE;
	}

	public reveal(size: number, isBroadcast?: boolean): void {
		if (size < 0) throw new RangeError(`reveal ${size} must be non-negative`);
		const current = this.cursor;
		if (size === current) return;
		if (size < current) throw new RangeError(`reveal ${size} is behind the cursor (size=${current}); reveal only moves forward`);

		// Broadcast reveals cover regions already persisted into the shared
		// buckets, so they're findable there — no per-worker staging needed, and
		// they advance `committedCursor` too (this IS the on-disk commit frontier,
		// e.g. seeded from the pinned size at recovery). A manual reveal exposes
		// this worker's own not-yet-persisted entries, so stage them (key ->
		// entryOffset) so get/has can see them before pin, but leave
		// `committedCursor` where it is — pin is what threads them into buckets.
		if (isBroadcast) {
			if (size > this.committedCursor) this.committedCursor = size;
		} else {
			for (let entryOffset = this.next(current); entryOffset < size;) {
				const recordOffset = entryOffset - POINTER_SIZE;
				const recordBytes = this.mmap(recordOffset);
				const keyOffset = POINTER_SIZE;
				const [, keySize] = this.key.decode(recordBytes, keyOffset);
				// Copy the key: the map holds keys by reference, but recordBytes is
				// a live view into the moving mmap and must not be retained.
				const keyBytes = recordBytes.slice(keyOffset, keyOffset + keySize);
				this.newEntries.set(keyBytes, entryOffset);
				const valueOffset = keyOffset + keySize;
				const [, valueSize] = this.value.decode(recordBytes, valueOffset);
				entryOffset = this.next(entryOffset + keySize + valueSize);
			}
		}

		this.cursor = size;
	}

	// `begin` is the record start (recordOffset) or entry start (entryOffset -
	// POINTER_SIZE, same thing); always reads a full record's worth of bytes.
	// Any explicit `begin` here MUST come from `next()` (directly or via a prior
	// `next()` step in a walk loop) so it's guaranteed to have `recordMaxSize`
	// room in its chunk — mmap() throws otherwise instead of silently truncating.
	public mmap(begin: number): Uint8Array {
		return this.entries.mmap(this.recordMaxSize, begin);
	}

	public stage(key: Codec.InferInput<Key>, value: Codec.InferInput<Value>, offset?: number): number {
		offset ??= this.next(this.size());
		// Only key+value get written here (the pointer slot before it is written
		// separately by commit/rehash), so this is bounded by maxEntrySize, not
		// the full recordMaxSize — asking mmap() for exactly what's being written
		// is what makes a bad manual `offset` throw instead of truncating silently.
		return this.entry.encodeInto({ key, value }, this.entries.mmap(this.maxEntrySize, offset));
	}

	// The next-in-chain pointer prepended to each record is read back with
	// `U64.decode` (big-endian). It MUST be written with the same codec — a raw
	// BigUint64Array view is little-endian on every platform we run on, so mixing
	// the two silently corrupted chain links (only zero/tail pointers survived,
	// which is why single-entry buckets appeared to work).
	private writePointer_(recordBytes: Uint8Array, value: bigint): void {
		U64.encodeInto(value, recordBytes, 0);
	}
	public commit(size: number): void {
		if (!this.commiter) throw new Error("persist on read-only store");
		// Commit threads every entry between the PERSIST high-water and `size` into
		// the shared buckets. It's keyed off `committedCursor`, not `cursor`: reveal
		// has usually already pulled `cursor` up to (or past) `size` by staging the
		// same entries into `newEntries`, so comparing against `cursor` would make
		// this a silent no-op and never persist the index.
		if (size === this.committedCursor) return;
		if (size < this.committedCursor) throw new RangeError(`persist ${size} can't be smaller than committed size`);
		const current = this.committedCursor;
		if (current < size && this.buckets.size() === 0) {
			throw new Error(`commit: hashmap "${this.path}" has zero buckets; bucket allocation is not initialised`);
		}
		let count = Number(this.counter.get(0));
		for (let entryOffset = this.next(current); entryOffset < size;) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			const bucket = hashKey(recordBytes, keyOffset, keyOffset + keySize) % this.buckets.size();
			const prevBucketValue = this.buckets.get(bucket);
			this.writePointer_(recordBytes, prevBucketValue);
			this.buckets.set(bucket, BigInt(entryOffset));
			count++;
			const valueOffset = keyOffset + keySize;
			const [, valueSize] = this.value.decode(recordBytes, valueOffset);
			const entrySize = keySize + valueSize;
			entryOffset = this.next(entryOffset + entrySize);
		}
		this.counter.set(0, BigInt(count));
		this.committedCursor = size;
		// Entries are now findable in the shared buckets, so once nothing is left
		// staged past `size` the whole map is redundant. But `cursor` (reveal
		// frontier) may already be PAST `size` here — a manual reveal can advance
		// it further while this commit is in flight (e.g. across the await gap
		// between the manifest's record-reveal and persist transactions). Those
		// entries in [size, cursor) are staged but NOT YET in buckets, so clearing
		// unconditionally would make them briefly invisible to get()/has() until
		// the next commit cycle happened to run — which read as "breaks randomly".
		// FastUint8ArrayMap has no per-key delete/iteration, so instead of pruning
		// just the committed subset, only clear once `cursor` has caught up to
		// `size` (i.e. nothing is pending beyond this commit); otherwise leave the
		// (now partly redundant, still fully correct) map for the next commit to
		// clear once it's safe. In the normal case cursor === size already, so
		// this clears every time exactly like before — it only holds back in the
		// rare race window, and self-corrects on the very next commit.
		if (size > this.cursor) this.cursor = size;
		if (this.cursor <= size) this.newEntries.clear();
		this.checkLoadFactor();
	}

	public truncate(size: number): void {
		if (!this.commiter) throw new Error("truncate on read-only store");
		if (size < 0) throw new RangeError(`truncate ${size} must be non-negative`);
		if (size > this.cursor) throw new RangeError(`truncate ${size} can't be bigger than current size`);
		// Unthread every PERSISTED entry above `size` from the shared buckets. This
		// walks the committed region (`committedCursor` down to `size`), so it's
		// keyed off `committedCursor`, not `cursor`. Any revealed-but-uncommitted
		// entries above `committedCursor` were never in the buckets — dropping
		// `cursor`/`newEntries` back to `size` is all they need.
		const current = this.committedCursor;
		if (size < current && this.buckets.size() === 0) {
			throw new Error(`truncate: hashmap "${this.path}" has zero buckets; bucket allocation is not initialised`);
		}
		let count = Number(this.counter.get(0));
		for (let entryOffset = this.next(size); entryOffset < current;) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const [candidateBucketValue] = U64.decode(recordBytes);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			const bucket = hashKey(recordBytes, keyOffset, keyOffset + keySize) % this.buckets.size();
			const currentBucketValue = this.buckets.get(bucket);
			if (candidateBucketValue < currentBucketValue) {
				this.buckets.set(bucket, candidateBucketValue);
			}
			count--;
			const valueOffset = keyOffset + keySize;
			const [, valueSize] = this.value.decode(recordBytes, valueOffset);
			const entrySize = keySize + valueSize;
			entryOffset = this.next(entryOffset + entrySize);
		}
		this.counter.set(0, BigInt(count));
		this.committedCursor = size;
		this.cursor = size;
		this.newEntries.clear();
		this.checkLoadFactor();
	}

	private checkLoadFactor(): void {
		const bucketCount = this.buckets.size();
		const factor = Number(this.counter.get(0)) / bucketCount;
		const { target, maxDrift } = this.loadFactor;
		if (factor <= target + maxDrift && factor >= target - maxDrift) return;
		const next = Math.max(INITIAL_BUCKET_COUNT, Math.ceil(Number(this.counter.get(0)) / target));
		if (next === bucketCount) return;
		this.rehash(next);
	}

	private rehash(bucketCount: number): void {
		console.log(`Rehasing with bucketCount=${bucketCount}, old=${this.buckets.size()}`);
		// SharedSlotArray.resize GROWS by its argument (it's an atomic add), so grow
		// by the delta to reach the absolute target. checkLoadFactor only ever grows
		// the table, so the delta is non-negative.
		const delta = bucketCount - this.buckets.size();
		if (delta > 0) this.buckets.resize(delta);
		for (let bucket = 0; bucket < bucketCount; bucket++) this.buckets.set(bucket, 0n);
		// Only the COMMITTED region lives in the buckets (and is counted). Rethread
		// exactly that; revealed-but-uncommitted entries above `committedCursor`
		// are served from `newEntries` and must not be threaded here.
		for (let entryOffset = this.next(0); entryOffset < this.committedCursor;) {
			const recordBytes = this.mmap(entryOffset - POINTER_SIZE);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			const bucket = hashKey(recordBytes, keyOffset, keyOffset + keySize) % bucketCount;
			this.writePointer_(recordBytes, this.buckets.get(bucket));
			this.buckets.set(bucket, BigInt(entryOffset));
			const valueOffset = keyOffset + keySize;
			const [, valueSize] = this.value.decode(recordBytes, valueOffset);
			entryOffset = this.next(entryOffset + keySize + valueSize);
		}
	}

	private encodeKey(key: Codec.InferInput<Key>): Uint8Array {
		const size = this.key.encodeInto(key, this.keyScratch_, 0);
		return this.keyScratch_.subarray(0, size);
	}

	public getKey(entryOffset: number): [Codec.InferOutput<Key>, number] {
		const recordBytes = this.mmap(entryOffset - POINTER_SIZE);
		const keyOffset = POINTER_SIZE;
		return this.key.decode(recordBytes, keyOffset);
	}

	public getEntry(entryOffset: number): [Codec.InferOutput<Key>, Codec.InferOutput<Value>] {
		const recordBytes = this.mmap(entryOffset - POINTER_SIZE);
		const keyOffset = POINTER_SIZE;
		const [key, keySize] = this.key.decode(recordBytes, keyOffset);
		const [value] = this.value.decode(recordBytes, keyOffset + keySize);
		return [key, value];
	}

	public get(key: Codec.InferInput<Key>): Codec.InferOutput<Value> | undefined {
		const encoded = this.encodeKey(key);
		const staged = this.newEntries.get(encoded);
		if (staged !== undefined) {
			const [, value] = this.getEntry(staged);
			return value;
		}
		const bucket = hashKey(encoded) % this.buckets.size();
		let entryOffset = Number(this.buckets.get(bucket));
		while (entryOffset !== 0) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			if (equals(encoded, recordBytes.subarray(keyOffset, keyOffset + keySize))) {
				const [value] = this.value.decode(recordBytes, keyOffset + keySize);
				return value;
			}
			const [next] = U64.decode(recordBytes);
			entryOffset = Number(next);
		}
		return undefined;
	}

	public getPointer(key: Codec.InferInput<Key>): number | undefined {
		const encoded = this.encodeKey(key);
		const staged = this.newEntries.get(encoded);
		if (staged !== undefined) return staged;
		const bucket = hashKey(encoded) % this.buckets.size();
		let entryOffset = Number(this.buckets.get(bucket));
		while (entryOffset !== 0) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			if (equals(encoded, recordBytes.subarray(keyOffset, keyOffset + keySize))) return entryOffset;
			const [next] = U64.decode(recordBytes);
			entryOffset = Number(next);
		}
		return undefined;
	}

	public getValueAndPointer(key: Codec.InferInput<Key>): [Codec.InferOutput<Value>, number] | undefined {
		const encoded = this.encodeKey(key);
		const staged = this.newEntries.get(encoded);
		if (staged !== undefined) {
			const [, value] = this.getEntry(staged);
			return [value, staged];
		}
		const bucket = hashKey(encoded) % this.buckets.size();
		let entryOffset = Number(this.buckets.get(bucket));
		while (entryOffset !== 0) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			if (equals(encoded, recordBytes.subarray(keyOffset, keyOffset + keySize))) {
				const [value] = this.value.decode(recordBytes, keyOffset + keySize);
				return [value, entryOffset];
			}
			const [next] = U64.decode(recordBytes);
			entryOffset = Number(next);
		}
		return undefined;
	}

	public has(key: Codec.InferInput<Key>): boolean {
		const encoded = this.encodeKey(key);
		if (this.newEntries.has(encoded)) return true;
		const bucket = hashKey(encoded) % this.buckets.size();
		let entryOffset = Number(this.buckets.get(bucket));
		while (entryOffset !== 0) {
			const recordOffset = entryOffset - POINTER_SIZE;
			const recordBytes = this.mmap(recordOffset);
			const keyOffset = POINTER_SIZE;
			const [, keySize] = this.key.decode(recordBytes, keyOffset);
			if (equals(encoded, recordBytes.subarray(keyOffset, keyOffset + keySize))) return true;
			const [next] = U64.decode(recordBytes);
			entryOffset = Number(next);
		}
		return false;
	}

	public sync(): void {
		this.entries.sync();
		this.buckets.sync();
		this.counter.sync();
	}

	public close(): void {
		this.entries.close();
		this.buckets.close();
		this.counter.close();
	}

	public [Symbol.dispose](): void {
		this.close();
	}
}

function hashKey(source: Uint8Array, begin: number = 0, end: number = source.length): number {
	let hash = 0x811c9dc5;
	for (let index = begin; index < end; index++) hash = Math.imul(hash ^ source[index]!, 0x01000193);
	return hash >>> 0;
}
