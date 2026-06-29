import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";
import { Codec, FixedCodec } from "@nomadshiba/codec";
import { StoreRocks } from "~/libs/storage/Store.ts";

export type KvStoreOptions<K extends FixedCodec, V extends Codec> = {
	rocksdb: RocksDatabase;
	key: K;
	value: V;
};

export class KvStore<K extends FixedCodec, V extends Codec> extends StoreRocks {
	public readonly rocksdb: RocksDatabase;
	public readonly key: K;
	public readonly value: V;

	private readonly keyBuf: Uint8Array<ArrayBuffer>;

	private constructor(options: KvStoreOptions<K, V>) {
		super();
		this.rocksdb = options.rocksdb;
		this.key = options.key;
		this.value = options.value;

		this.keyBuf = new Uint8Array(this.key.stride.size);
	}

	static open<K extends FixedCodec, V extends Codec>(options: KvStoreOptions<K, V>): KvStore<K, V> {
		const self = new KvStore(options);
		return self;
	}

	get(key: Codec.InferInput<K>, transaction?: Transaction): Codec.InferOutput<V> | undefined {
		this.key.encodeInto(key, this.keyBuf);
		const bytes = this.rocksdb.getSync(this.keyBuf, { transaction });
		if (!bytes) return undefined; // tombstone (null) or rocksdb miss
		return this.value.decodeValue(bytes);
	}

	set(key: Codec.InferInput<K>, value: Codec.InferInput<V>, transaction: Transaction) {
		this.key.encodeInto(key, this.keyBuf);
		this.rocksdb.putSync(this.keyBuf, this.value.encode(value), { transaction });
	}

	del(key: Codec.InferInput<K>, transaction: Transaction) {
		this.key.encodeInto(key, this.keyBuf);
		this.rocksdb.removeSync(this.keyBuf, { transaction });
	}

	clear(): void {
		this.rocksdb.clearSync();
	}
}
