import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";
import { Codec, FixedCodec } from "@nomadshiba/codec";
import { BloomFilter, BloomFilterOptions } from "~/libs/collections/BloomFilter.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { Batch, FlushFinalizer, StoreRocks } from "~/libs/storage/Store.ts";

export type KvStoreOptions<K extends FixedCodec, V extends FixedCodec> = {
	prefix: Uint8Array;
	rocksdb: RocksDatabase;
	key: K;
	value: V;
	/**
	 * Opt-in negative-lookup gate. When set, open() scans this store's prefix
	 * range in RocksDB to seed the filter, and get() skips the RocksDB read for
	 * keys the filter rules out. Best for low-churn / append-mostly stores. The
	 * scan is a one-time startup cost proportional to the live key count.
	 */
	bloom?: BloomFilterOptions;
};

export interface KvStoreBatch<K extends FixedCodec, V extends FixedCodec> extends Batch {
	get(key: Codec.InferInput<K>): Promise<Codec.InferOutput<V> | undefined>;
	set(key: Codec.InferInput<K>, value: Codec.InferInput<V>): void;
	delete(key: Codec.InferInput<K>): void;
}

export class KvStore<K extends FixedCodec, V extends FixedCodec> extends StoreRocks<KvStoreBatch<K, V>> {
	public readonly rocksdb: RocksDatabase;
	public readonly key: K;
	public readonly value: V;
	public readonly prefix: Uint8Array;

	private staged: FastUint8ArrayMap<Uint8Array | null>;
	// The snapshot being flushed. Reads must consult it (it's the most recent
	// committed-to-this-flush data) and flush() drains it; the finalizer clears
	// it. Concurrent applies during a flush go to staged, never here, so they
	// survive — unlike before, where the finalizer replaced the live staged and
	// silently dropped anything applied mid-flush.
	private frozen: FastUint8ArrayMap<Uint8Array | null> | null = null;

	// Add-only superset of every key currently in RocksDB. Gates get()'s RocksDB
	// read: if the filter says "definitely absent" we skip the read entirely.
	// MUST stay a superset of RocksDB (see open() for the repopulation), or reads
	// will false-negative live keys. null when disabled.
	private readonly bloom: BloomFilter | null;

	// Reusable buffer for encoding prefixed keys. get(), flush(), and
	// batch.get() are never concurrent, so a single shared buffer is safe.
	private readonly keyBuf: Uint8Array<ArrayBuffer>;
	private readonly encodedKeyView: Uint8Array<ArrayBuffer>;

	private constructor(options: KvStoreOptions<K, V>) {
		super();
		this.rocksdb = options.rocksdb;
		this.key = options.key;
		this.value = options.value;
		this.prefix = options.prefix;

		this.staged = new FastUint8ArrayMap();
		this.bloom = options.bloom ? new BloomFilter(options.bloom) : null;

		this.keyBuf = new Uint8Array(this.key.stride.size + this.prefix.length);
		this.keyBuf.set(this.prefix);
		this.encodedKeyView = this.keyBuf.subarray(this.prefix.length);
	}

	static async open<K extends FixedCodec, V extends FixedCodec>(options: KvStoreOptions<K, V>): Promise<KvStore<K, V>> {
		const self = new KvStore(options);

		// Seed the bloom from what's already on disk so it's a superset from the
		// first read. Rebuilding on every open also sheds bits for keys deleted in
		// past sessions (RocksDB only returns live keys), which keeps the FP rate
		// from drifting upward over the store's lifetime.
		if (self.bloom) {
			const end = prefixUpperBound(self.prefix);
			const range = end === undefined
				? self.rocksdb.getRange({ start: self.prefix })
				: self.rocksdb.getRange({ start: self.prefix, end });
			const cut = self.prefix.length;
			for (const { key } of range) {
				const k = key as Uint8Array;
				self.bloom.add(cut ? k.subarray(cut) : k);
			}
		}

		return self;
	}

	freeze(): void {
		if (this.frozen) return;
		this.frozen = this.staged;
		this.staged = new FastUint8ArrayMap();
	}

	async get(key: Codec.InferInput<K>): Promise<Codec.InferOutput<V> | undefined> {
		this.key.encodeInto(key, this.encodedKeyView);

		// Freshness: staged > frozen (being flushed) > rocksdb. We resolve layer by
		// layer instead of with `??` so a tombstone (value === null) at an upper
		// layer correctly shadows lower layers — `??` treated null as "not found"
		// and leaked stale values through a pending delete.
		let bytes: Uint8Array | null | undefined = this.staged.get(this.encodedKeyView);
		if (bytes === undefined && this.frozen) bytes = this.frozen.get(this.encodedKeyView);
		if (bytes === undefined) {
			if (this.bloom && !this.bloom.mightContain(this.encodedKeyView)) return undefined;
			bytes = await this.rocksdb.get(this.keyBuf);
		}

		if (!bytes) return undefined; // tombstone (null) or rocksdb miss
		return this.value.decodeValue(bytes);
	}

	batch(): KvStoreBatch<K, V> {
		const batch = new FastUint8ArrayMap<Uint8Array | null>();

		const set: KvStoreBatch<K, V>["set"] = (key, value) => {
			batch.set(this.key.encode(key), this.value.encode(value));
		};

		const del: KvStoreBatch<K, V>["delete"] = (key) => {
			batch.set(this.key.encode(key), null);
		};

		const get: KvStoreBatch<K, V>["get"] = async (key) => {
			this.key.encodeInto(key, this.encodedKeyView);
			// Freshness: batch > staged > frozen > rocksdb. Layer-by-layer for the
			// same tombstone-correctness reason as KvStore.get above.
			let bytes: Uint8Array | null | undefined = batch.get(this.encodedKeyView);
			if (bytes === undefined) bytes = this.staged.get(this.encodedKeyView);
			if (bytes === undefined && this.frozen) bytes = this.frozen.get(this.encodedKeyView);
			if (bytes === undefined) {
				if (this.bloom && !this.bloom.mightContain(this.encodedKeyView)) return undefined;
				bytes = await this.rocksdb.get(this.keyBuf);
			}

			if (!bytes) return undefined;
			return this.value.decodeValue(bytes);
		};

		const apply: KvStoreBatch<K, V>["apply"] = () => {
			for (const [key, bytes] of batch.entries()) {
				this.staged.setOwned(key, bytes);
			}
		};

		const discard: KvStoreBatch<K, V>["discard"] = () => {
			batch.clear();
		};

		return { set, delete: del, get, apply, discard };
	}

	async flush(trx: Transaction): Promise<FlushFinalizer> {
		// Standalone callers (and the test suite) flush without a separate freeze;
		// Atomic freezes everything up front, so here frozen is already set and
		// this is a no-op. Either way we drain a stable snapshot, never live staged.
		if (!this.frozen) this.freeze();
		const frozen = this.frozen!;
		await Promise.all(
			frozen.entries().map(async ([encodedKey, bytes]) => {
				this.keyBuf.set(encodedKey, this.prefix.length);
				if (!bytes) {
					await trx.remove(this.keyBuf);
					// Note: we can't unset the bloom bit for a deleted key. It stays a
					// (harmless) false positive until the next open() rebuild.
				} else {
					await trx.put(this.keyBuf, bytes);
					// Add only puts; the key is now in RocksDB so the filter must cover
					// it. Idempotent under transaction-callback replay.
					this.bloom?.add(encodedKey);
				}
			}),
		);
		// Draining from frozen (not clearing until the finalizer) also makes a
		// transaction-callback replay safe: a retry just re-puts the same snapshot.
		return () => {
			this.frozen = null;
		};
	}

	async clear(): Promise<void> {
		await this.rocksdb.clear();
		this.staged.clear();
		this.frozen = null;
		this.bloom?.clear();
	}
}

/**
 * Exclusive upper bound for a prefix scan: the prefix with its last
 * non-0xff byte incremented. Returns undefined when the prefix is all 0xff
 * (or empty), meaning "no upper bound — scan to the end".
 */
function prefixUpperBound(prefix: Uint8Array): Uint8Array | undefined {
	const end = prefix.slice();
	for (let i = end.length - 1; i >= 0; i--) {
		if (end[i]! < 0xff) {
			end[i]!++;
			return end.subarray(0, i + 1);
		}
	}
	return undefined;
}
