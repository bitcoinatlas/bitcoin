import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";
import { Codec, FixedCodec } from "@nomadshiba/codec";
import { equals } from "@std/bytes/equals";
import { Store } from "~/libs/storage/Store.ts";

/**
 * Outcome of a {@link KvStore.setNoOverwrite}:
 * - `written`    — key was absent, value written.
 * - `idempotent` — key already held EXACTLY this value (a harmless replay).
 * - `conflict`   — key already held a DIFFERENT value (for the spender index,
 *                  this is a double spend / corruption — the caller decides).
 */
export type PutGuard = "written" | "idempotent" | "conflict";

export type KvStoreOptions<K extends Codec, V extends Codec> = {
	rocksdb: RocksDatabase;
	key: K;
	value: V;
};

export class KvStore<K extends Codec, V extends Codec> extends Store {
	public readonly rocksdb: RocksDatabase;
	public readonly key: K;
	public readonly value: V;

	private constructor(options: KvStoreOptions<K, V>) {
		super();
		this.rocksdb = options.rocksdb;
		this.key = options.key;
		this.value = options.value;
	}

	static open<K extends FixedCodec, V extends Codec>(options: KvStoreOptions<K, V>): KvStore<K, V> {
		const self = new KvStore(options);
		return self;
	}

	get(key: Codec.InferInput<K>, transaction?: Transaction): Codec.InferOutput<V> | undefined {
		const bytes = this.rocksdb.getSync(this.key.encode(key), { transaction });
		if (!bytes) return undefined; // tombstone (null) or rocksdb miss
		const [value] = this.value.decode(bytes);
		return value;
	}

	async getAsync(key: Codec.InferInput<K>, transaction?: Transaction): Promise<Codec.InferOutput<V> | undefined> {
		const bytes = await this.rocksdb.get(this.key.encode(key), { transaction });
		if (!bytes) return undefined; // tombstone (null) or rocksdb miss
		const [value] = this.value.decode(bytes);
		return value;
	}

	set(key: Codec.InferInput<K>, value: Codec.InferInput<V>, transaction: Transaction) {
		this.rocksdb.putSync(this.key.encode(key), this.value.encode(value), { transaction });
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
	setMany(entries: readonly (readonly [Codec.InferInput<K>, Codec.InferInput<V>])[], transaction: Transaction) {
		const n = entries.length;
		if (n === 0) return;

		const encoded: [Uint8Array, Uint8Array][] = new Array(n);
		for (let i = 0; i < n; i++) {
			const [key, value] = entries[i]!;
			encoded[i] = [this.value.encode(key), this.value.encode(value)];
		}

		this.rocksdb.putManySync(encoded, { transaction });
	}

	/**
	 * Put only if the key is absent; otherwise report whether the existing bytes
	 * match what we'd write. This is the parallel double-spend check from
	 * NOTES.md §3 — every input claims its prevOut exactly once, so a `written`
	 * is the norm, an `idempotent` means we're re-doing a range after a restart
	 * (the checkpoint didn't advance past writes that did land), and a `conflict`
	 * means two different txs claim the same output: a double spend.
	 *
	 * Compares RAW stored bytes against the freshly-encoded value, so it stays
	 * codec-agnostic and never has to decode the existing entry.
	 *
	 * NOTE: the native binding exposes a real `noOverwrite` put option but the
	 * current vendored build doesn't forward it to `putSync`, so this does a
	 * read-then-put. The read is a bloom-filtered point lookup that MISSES on the
	 * common forward path (fresh key), so it's cheap; when the binding wires
	 * `noOverwrite` through, this collapses to a single put with the get only on
	 * the rare conflict.
	 */
	setNoOverwrite(key: Codec.InferInput<K>, value: Codec.InferInput<V>): PutGuard {
		const keyBuffer = this.key.encode(key);
		const existing = this.rocksdb.getSync(keyBuffer) as Uint8Array | undefined;
		const encoded = this.value.encode(value);
		if (!existing) {
			this.rocksdb.putSync(keyBuffer, encoded);
			return "written";
		}
		return equals(existing, encoded) ? "idempotent" : "conflict";
	}

	clear(): void {
		this.rocksdb.clearSync();
	}
}
