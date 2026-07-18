import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";
import { Codec, FixedCodec } from "@nomadshiba/codec";
import { Store } from "~/libs/storage/Store.ts";

export type KvStoreOptions<K extends FixedCodec, V extends Codec> = {
	rocksdb: RocksDatabase;
	key: K;
	value: V;
};

export class KvStore<K extends FixedCodec, V extends Codec> extends Store {
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
		const [value] = this.value.decode(bytes);
		return value;
	}

	set(key: Codec.InferInput<K>, value: Codec.InferInput<V>, transaction: Transaction) {
		this.key.encodeInto(key, this.keyBuf);
		this.rocksdb.putSync(this.keyBuf, this.value.encode(value), { transaction });
	}

	/**
	 * Batched put: writes every `[key, value]` pair in a single native call
	 * (one FFI crossing for the whole batch instead of one per entry). Semantics
	 * match calling {@link set} for each pair inside the same transaction.
	 *
	 * Unlike {@link set}, this cannot reuse the shared `keyBuf`: `putManySync`
	 * reads every entry only after this loop returns, so each key/value must own
	 * its own bytes. Keys share one backing allocation via non-overlapping views;
	 * values are copied out in case `value.encode()` hands back a reused buffer.
	 */
	setMany(
		entries: readonly (readonly [Codec.InferInput<K>, Codec.InferInput<V>])[],
		transaction: Transaction,
	) {
		const n = entries.length;
		if (n === 0) return;

		const stride = this.key.stride.size;
		const keyBacking = new Uint8Array(n * stride);
		const encoded: [Uint8Array, Uint8Array][] = new Array(n);
		for (let i = 0; i < n; i++) {
			const entry = entries[i]!;
			const keyBytes = keyBacking.subarray(i * stride, i * stride + stride);
			this.key.encodeInto(entry[0], keyBytes);
			const value = this.value.encode(entry[1]);
			encoded[i] = [keyBytes, Uint8Array.prototype.slice.call(value)];
		}

		this.rocksdb.putManySync(encoded, { transaction });
	}

	del(key: Codec.InferInput<K>, transaction: Transaction) {
		this.key.encodeInto(key, this.keyBuf);
		this.rocksdb.removeSync(this.keyBuf, { transaction });
	}

	clear(): void {
		this.rocksdb.clearSync();
	}
}
