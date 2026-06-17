import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";
import { Codec, FixedCodec } from "@nomadshiba/codec";
import { Batch, FlushFinalizer, StoreRocks } from "~/storage/Store.ts";
import { Uint8ArrayMap } from "~/utils/Uint8ArrayMap.ts";

export type KvStoreOptions<K extends FixedCodec, V extends FixedCodec> = {
	prefix: Uint8Array;
	rocksdb: RocksDatabase;
	key: K;
	value: V;
};

export interface KvStoreBatch<K extends FixedCodec, V extends FixedCodec> extends Batch {
	get(key: Codec.InferInput<K>): Promise<Codec.InferOutput<V> | undefined>;
	set(key: Codec.InferInput<K>, value: Codec.InferInput<V>): void;
}

export class KvStore<K extends FixedCodec, V extends FixedCodec> extends StoreRocks<KvStoreBatch<K, V>> {
	public readonly rocksdb: RocksDatabase;
	public readonly key: K;
	public readonly value: V;
	public readonly prefix: Uint8Array;

	private _staged: Uint8ArrayMap<Uint8Array>;
	// The snapshot being flushed. Reads must consult it (it's the most recent
	// committed-to-this-flush data) and flush() drains it; the finalizer clears
	// it. Concurrent applies during a flush go to _staged, never here, so they
	// survive — unlike before, where the finalizer replaced the live _staged and
	// silently dropped anything applied mid-flush.
	private _frozen: Uint8ArrayMap<Uint8Array> | null = null;

	private constructor(options: KvStoreOptions<K, V>) {
		super();
		this.rocksdb = options.rocksdb;
		this.key = options.key;
		this.value = options.value;
		this.prefix = options.prefix;

		this._staged = new Uint8ArrayMap();
	}

	static async open<K extends FixedCodec, V extends FixedCodec>(options: KvStoreOptions<K, V>): Promise<KvStore<K, V>> {
		const self = new KvStore(options);

		return self;
	}

	freeze(): void {
		if (this._frozen) return;
		this._frozen = this._staged;
		this._staged = new Uint8ArrayMap();
	}

	async get(key: Codec.InferInput<K>): Promise<Codec.InferOutput<V> | undefined> {
		const keyBytes = new Uint8Array(this.key.stride.size + this.prefix.length);
		keyBytes.set(this.prefix);
		this.key.encode(key, keyBytes.subarray(this.prefix.length));
		const encodedKey = keyBytes.subarray(this.prefix.length);
		// Freshness: staged > frozen (being flushed) > rocksdb.
		const bytes = this._staged.get(encodedKey) ?? this._frozen?.get(encodedKey) ?? await this.rocksdb.get(keyBytes);
		if (!bytes) return undefined;
		return this.value.decodeValue(bytes);
	}

	batch(): KvStoreBatch<K, V> {
		const batch = new Uint8ArrayMap<Uint8Array>();

		const set: KvStoreBatch<K, V>["set"] = (key, value) => {
			batch.set(this.key.encode(key), this.value.encode(value));
		};

		const get: KvStoreBatch<K, V>["get"] = async (key) => {
			const keyBytes = new Uint8Array(this.key.stride.size + this.prefix.length);
			keyBytes.set(this.prefix);
			this.key.encode(key, keyBytes.subarray(this.prefix.length));
			const encodedKey = keyBytes.subarray(this.prefix.length);
			// Freshness: batch > staged > frozen > rocksdb.
			const bytes = batch.get(encodedKey) ?? this._staged.get(encodedKey) ?? this._frozen?.get(encodedKey) ??
				await this.rocksdb.get(keyBytes);
			if (!bytes) return undefined;
			return this.value.decodeValue(bytes);
		};

		const apply: KvStoreBatch<K, V>["apply"] = () => {
			for (const [key, bytes] of batch.entries()) {
				this._staged.set(key, bytes);
			}
		};

		const discard: KvStoreBatch<K, V>["discard"] = () => {
			batch.clear();
		};

		return { set, get, apply, discard };
	}

	async flush(trx: Transaction): Promise<FlushFinalizer> {
		// Standalone callers (and the test suite) flush without a separate freeze;
		// Atomic freezes everything up front, so here _frozen is already set and
		// this is a no-op. Either way we drain a stable snapshot, never live staged.
		if (!this._frozen) this.freeze();
		const frozen = this._frozen!;
		for (const [encodedKey, bytes] of frozen.entries()) {
			const keyBytes = new Uint8Array(this.key.stride.size + this.prefix.length);
			keyBytes.set(this.prefix);
			keyBytes.set(encodedKey, this.prefix.length);
			await trx.put(keyBytes, bytes);
		}
		// Draining from _frozen (not clearing until the finalizer) also makes a
		// transaction-callback replay safe: a retry just re-puts the same snapshot.
		return () => {
			this._frozen = null;
		};
	}

	async clear(): Promise<void> {
		await this.rocksdb.clear();
		this._staged.clear();
		this._frozen = null;
	}
}
