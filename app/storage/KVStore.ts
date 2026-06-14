import { RocksDatabase, RocksDatabaseOptions, Store as RocksStore } from "@harperfast/rocksdb-js";
import { FixedCodec } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import type { Batch, Store, WAL } from "~/storage/Store.ts";
import { Uint8ArrayMap } from "~/utils/Uint8ArrayMap.ts";
import { Uint8ArrayView } from "~/utils/Uint8ArrayView.ts";

RocksDatabase.config({ blockCacheSize: 536870912 * 2 });

/**
 * A persistent key-value store backed by RocksDB.
 *
 * Fixed-stride keys and values. The staging + WAL layer on top of RocksDB exists so this
 * store participates in the multi-store atomic flush (the Store/WAL/atomic contract) — all
 * stores commit at one consistent cut. RocksDB's own WAL only protects what has already
 * reached RocksDB; our data.wal protects committed-but-not-yet-flushed staged entries.
 *
 * Layering (newest wins on read):
 *
 *   batch (pending)  →  staged (committed batches, not flushed)  →  frozen (mid-flush)  →  RocksDB
 *
 * Non-blocking flush (same shape as ArrayStore/BlobStore): createWAL() freezes the staged
 * map into an immutable `#frozen` and installs a fresh empty `#staged` in the same tick, so
 * new batches proceed while the flush writes to RocksDB. `#frozen` serves reads of the
 * in-flight keys from memory until discard(); apply() only writes them into RocksDB. Because
 * a key being applied is served from `#frozen`, a concurrent db.get never races the write.
 *
 * WAL format: [u32 entryCount LE]([key][value] * entryCount), fixed stride.
 * apply() replays the entries into RocksDB in a single transaction — idempotent (re-putting
 * the same key/value is a no-op rewrite), so a crash mid-apply is healed by replaying.
 *
 * Reorg note: this store has no positional truncate and no delete — only clear(). Reverting
 * keyed entries on a reorg needs a delete/tombstone story that is not implemented here.
 *
 * Consistency note: reads are snapshot-consistent with respect to flush. A single writer is
 * assumed (one batch at a time, enforced).
 */
export interface KVStoreBatch<K, V> extends Batch {
	get(key: K): Promise<V | undefined>;
	set(key: K, value: V): void;
}

export type KVStoreOptions<K, V> = {
	path: string;
	keyCodec: FixedCodec<K>;
	valueCodec: FixedCodec<V>;
	/** Kept for backward compatibility; no longer used (RocksDB manages sharding internally). */
	shards?: number;
};

const WAL_HEADER = 4; // bytes reserved at the front of the WAL buffer for the u32 entryCount

/**
 * Passthrough store so keys/values are stored as raw bytes without RocksDB's own
 * (msgpack-like) serialization — our codecs already produce the encoded bytes.
 *
 * The RocksDB binding reuses a single shared 65536-byte buffer for all db.get() results
 * and returns a reference to it (byteLength always 65536). `decodeValue` is called
 * synchronously during resolution, at which point the buffer holds the correct bytes.
 * We must copy `valueStride` bytes here — the returned Uint8Array from db.get() would
 * otherwise be overwritten by the next get() call before the caller can read it.
 */
// deno-lint-ignore no-explicit-any
class BinaryStore extends RocksStore {
	readonly #valueStride: number;

	constructor(path: string, options: RocksDatabaseOptions, valueStride: number) {
		super(path, options);
		this.#valueStride = valueStride;
	}

	override encodeKey(key: Uint8Array): any {
		return key;
	}
	override decodeKey(key: any): Uint8Array {
		return key;
	}
	override encodeValue(value: Uint8Array): any {
		return value;
	}
	override decodeValue(value: any): Uint8Array {
		// Copy the value bytes out of the binding's reused 65536-byte read buffer at resolution
		// time. This copy is what makes concurrent reads safe: the returned Uint8Array is fully
		// detached from the shared buffer, so a later db.get() overwriting that buffer cannot
		// corrupt a value the caller is still holding. Must be slice() (a copy), not subarray()
		// (a view), or the detachment guarantee is lost.
		return value.slice(0, this.#valueStride);
	}
}

export class KVStore<K, V> implements Store<KVStoreBatch<K, V>>, Disposable {
	// deno-lint-ignore no-explicit-any
	readonly #db: RocksDatabase; // RocksDatabase handle
	readonly #keyCodec: FixedCodec<K>;
	readonly #valueCodec: FixedCodec<V>;
	readonly #keyStride: number;
	readonly #valueStride: number;
	readonly #entryStride: number;
	readonly #walPath: string;

	/** Committed-but-unflushed entries: key bytes → value bytes. */
	#staged: Uint8ArrayMap<Uint8Array>;
	/** Set during a flush; serves reads of the in-flight keys until discard(). null when clean. */
	#frozen: Uint8ArrayMap<Uint8Array> | null = null;

	#batchOpen = false;
	#closed = false;

	wal: WAL | null = null;

	private constructor(
		// deno-lint-ignore no-explicit-any
		db: RocksDatabase,
		keyCodec: FixedCodec<K>,
		valueCodec: FixedCodec<V>,
		walPath: string,
	) {
		this.#db = db;
		this.#keyCodec = keyCodec;
		this.#valueCodec = valueCodec;
		this.#keyStride = keyCodec.stride.size;
		this.#valueStride = valueCodec.stride.size;
		this.#entryStride = this.#keyStride + this.#valueStride;
		this.#walPath = walPath;
		this.#staged = new Uint8ArrayMap<Uint8Array>(256);
	}

	static async open<K, V>(options: KVStoreOptions<K, V>): Promise<KVStore<K, V>> {
		const { path, keyCodec, valueCodec } = options;
		await Deno.mkdir(path, { recursive: true });
		const walPath = join(path, "data.wal");

		const rocksOptions: RocksDatabaseOptions = {
			parallelismThreads: Math.max(1, Math.floor(navigator.hardwareConcurrency * .5)),
			maxKeySize: keyCodec.stride.size,
			// enableStats: true,
		};
		const rocksStore = new BinaryStore(join(path, "rocksdb"), rocksOptions, valueCodec.stride.size);
		const db = RocksDatabase.open(rocksStore);

		const store = new KVStore(db, keyCodec, valueCodec, walPath);

		// A WAL on disk means a flush was interrupted: staged entries were recorded but may
		// not have reached RocksDB. Expose it for recovery — reads are stale until applied.
		if (await exists(walPath)) {
			const buf = await Deno.readFile(walPath);
			store.wal = store.#makeWal(buf);
		}

		globalThis.addEventListener("unload", () => {
			if (store.#closed) return;
			try {
				const miss = Number(store.#db.getStat("rocksdb.block.cache.miss"));
				const hit = Number(store.#db.getStat("rocksdb.block.cache.hit"));
				const bloomUseful = store.#db.getStat("rocksdb.bloom.filter.useful");
				const bloomFull = store.#db.getStat("rocksdb.bloom.filter.full.positive");
				const total = (hit ?? 0) + (miss ?? 0);
				const hitRate = total > 0 ? ((hit ?? 0) / total * 100).toFixed(1) : "n/a";
				console.log(
					`[rocksdb:${store.#walPath}] cache hit=${hit} miss=${miss} rate=${hitRate}% bloom.useful=${bloomUseful} bloom.full=${bloomFull}`,
				);
			} catch {
				/*  */
			}
		});

		return store;
	}

	#assertOpen(): void {
		if (this.#closed) throw new Error("Store is closed");
	}

	async #getByBytes(
		keyBytes: Uint8Array,
		batchLookup: ((k: Uint8Array) => Uint8Array | undefined) | null,
	): Promise<V | undefined> {
		// Snapshot the in-memory layers synchronously so the read stays consistent across the
		// db.get await even if a flush starts/finishes mid-read (freeze swaps objects).
		const staged = this.#staged;
		const frozen = this.#frozen;

		if (batchLookup) {
			const v = batchLookup(keyBytes);
			if (v !== undefined) return this.#valueCodec.decode(v)[0];
		}
		const s = staged.get(keyBytes);
		if (s !== undefined) return this.#valueCodec.decode(s)[0];
		if (frozen) {
			const f = frozen.get(keyBytes);
			if (f !== undefined) return this.#valueCodec.decode(f)[0];
		}

		// db.get() is hybrid: returns the value synchronously on a memtable/block-cache hit,
		// otherwise a Promise. BinaryStore.decodeValue already slices a private copy out of the
		// binding's shared read buffer during resolution, so the bytes we hold are never aliased
		// to that buffer — the result is safe to keep across any later get(). This is what lets
		// reads run concurrently: there is no live reference into the reused buffer to race on.
		const raw = this.#db.get(keyBytes) as Uint8Array | Promise<Uint8Array | undefined> | undefined;
		const resolved = raw instanceof Promise ? await raw : raw;
		if (resolved == null) return undefined;
		return this.#valueCodec.decode(resolved)[0];
	}

	async get(key: K): Promise<V | undefined> {
		this.#assertOpen();
		return await this.#getByBytes(this.#keyCodec.encode(key), null);
	}

	batch(): KVStoreBatch<K, V> {
		this.#assertOpen();
		if (this.#batchOpen) throw new Error("A batch is already open");
		this.#batchOpen = true;

		const keyStride = this.#keyStride;
		const entryStride = this.#entryStride;

		// Active-batch entries, packed flat: [key0|val0][key1|val1]…
		// batchIndex maps key bytes → byte offset, for intra-batch get() and in-place
		// overwrite on duplicate set() (sound because values are fixed-stride).
		let pendingBuf = new Uint8Array(entryStride * 64);
		let pendingCount = 0;
		const batchIndex = new Uint8ArrayMap<number>(64);
		let live = true;

		const lookup = (k: Uint8Array): Uint8Array | undefined => {
			const off = batchIndex.get(k);
			if (off === undefined) return undefined;
			return pendingBuf.subarray(off + keyStride, off + entryStride);
		};

		const close = () => {
			live = false;
			this.#batchOpen = false;
		};

		return {
			get: async (key: K): Promise<V | undefined> => {
				if (!live) throw new Error("Batch already settled");
				return await this.#getByBytes(this.#keyCodec.encode(key), lookup);
			},
			set: (key: K, value: V): void => {
				if (!live) throw new Error("Batch already settled");
				const kBytes = this.#keyCodec.encode(key);
				const vBytes = this.#valueCodec.encode(value);

				const existing = batchIndex.get(kBytes);
				if (existing !== undefined) {
					pendingBuf.set(vBytes, existing + keyStride); // in-place overwrite (fixed stride)
					return;
				}

				const needed = (pendingCount + 1) * entryStride;
				if (needed > pendingBuf.length) {
					const next = new Uint8Array(Math.max(needed, pendingBuf.length * 2));
					next.set(pendingBuf);
					pendingBuf = next;
				}

				const off = pendingCount * entryStride;
				pendingBuf.set(kBytes, off);
				pendingBuf.set(vBytes, off + keyStride);
				batchIndex.set(kBytes, off);
				pendingCount++;
			},
			apply: (): void => {
				if (!live) throw new Error("Batch already settled");
				for (let i = 0; i < pendingCount; i++) {
					const off = i * entryStride;
					const k = pendingBuf.subarray(off, off + keyStride);
					const v = pendingBuf.subarray(off + keyStride, off + entryStride);
					this.#staged.set(k, v);
				}
				close();
			},
			discard: (): void => {
				if (!live) return;
				close();
			},
		};
	}

	async flush(): Promise<void> {
		const wal = await this.createWAL();
		await wal.apply();
		await wal.discard();
	}

	async createWAL(): Promise<WAL> {
		this.#assertOpen();
		if (this.#batchOpen) throw new Error("Can't start a flush while a batch is open");
		if (this.#frozen || this.wal) throw new Error("A flush is already in progress");

		// Freeze synchronously: the old staged map becomes immutable, new batches get a fresh one.
		const frozen = this.#staged;
		this.#frozen = frozen;
		this.#staged = new Uint8ArrayMap<Uint8Array>(256);

		// Serialize frozen → WAL buffer.
		const entryCount = frozen.size;
		const walBuf = new Uint8Array(WAL_HEADER + entryCount * this.#entryStride);
		new Uint8ArrayView(walBuf).setUint32(0, entryCount);
		let pos = WAL_HEADER;
		for (const [k, v] of frozen) {
			walBuf.set(k, pos);
			walBuf.set(v, pos + this.#keyStride);
			pos += this.#entryStride;
		}

		await Deno.writeFile(this.#walPath, walBuf, { create: true });

		this.wal = this.#makeWal(walBuf);
		return this.wal;
	}

	#makeWal(buffer: Uint8Array): WAL {
		const apply = async (): Promise<void> => {
			await this.#applyBuffer(buffer);
			// Does NOT touch #staged (the fresh layer) or #frozen (reads still served from it).
		};
		const discard = async (): Promise<void> => {
			this.#frozen = null;
			this.wal = null;
			await Deno.remove(this.#walPath).catch(() => {});
		};
		return { apply, discard };
	}

	async #applyBuffer(buffer: Uint8Array): Promise<void> {
		const view = new Uint8ArrayView(buffer);
		const entryCount = view.getUint32(0);
		if (entryCount === 0) return;

		await this.#db.transaction(async (txn: { put(k: Uint8Array, v: Uint8Array): Promise<void> }) => {
			let pos = WAL_HEADER;
			for (let i = 0; i < entryCount; i++) {
				// .slice() copies the bytes into a new ArrayBuffer — required because the RocksDB
				// binding reads the full underlying ArrayBuffer rather than honouring byteOffset/
				// byteLength of a subarray view, so shared-buffer subarrays produce wrong data.
				const keySlice = buffer.slice(pos, pos + this.#keyStride);
				const valSlice = buffer.slice(pos + this.#keyStride, pos + this.#entryStride);
				await txn.put(keySlice, valSlice);
				pos += this.#entryStride;
			}
		});
	}

	async clear(): Promise<void> {
		this.#assertOpen();
		if (this.#batchOpen) throw new Error("Can't clear while a batch is open");
		if (this.#frozen || this.wal) throw new Error("Can't clear while a flush is in progress");
		this.#staged = new Uint8ArrayMap<Uint8Array>(256);
		await this.#db.clear();
	}

	close(): void {
		if (this.#closed) return;
		if (this.#batchOpen) throw new Error("Can't close while a batch is open");
		if (this.#frozen || this.wal) throw new Error("Can't close while a flush is in progress");
		this.#closed = true;
		this.#db.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
