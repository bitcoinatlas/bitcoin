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

	async get(key: Codec.InferInput<K>): Promise<Codec.InferOutput<V> | undefined> {
		const keyBytes = new Uint8Array(this.key.stride.size + this.prefix.length);
		keyBytes.set(this.prefix);
		this.key.encode(key, keyBytes.subarray(this.prefix.length));
		const bytes = this._staged.get(key) ?? await this.rocksdb.get(keyBytes);
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
			const bytes = batch.get(key) ?? this._staged.get(key) ?? await this.rocksdb.get(keyBytes);
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

	private _flushing: boolean = false;
	async flush(trx: Transaction): Promise<FlushFinalizer> {
		if (this._flushing) throw new Error("you are already flushing rn");
		this._flushing = true;
		for (const [key, bytes] of this._staged.entries()) {
			const keyBytes = new Uint8Array(this.key.stride.size + this.prefix.length);
			keyBytes.set(this.prefix);
			this.key.encode(key, keyBytes.subarray(this.prefix.length));
			await trx.put(keyBytes, bytes);
		}
		return () => {
			this._staged = new Uint8ArrayMap();
			this._flushing = false;
		};
	}

	async clear(): Promise<void> {
		await this.rocksdb.clear();
		this._staged.clear();
	}
}
