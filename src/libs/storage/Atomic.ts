import { RocksDatabase } from "@harperfast/rocksdb-js";
import { randomBytes } from "@noble/hashes/utils";
import { BytesCodec, TupleCodec, U64 } from "@nomadshiba/codec";
import { equals } from "@std/bytes";
import { existsSync } from "@std/fs";
import { join } from "@std/path";
import { writeFileSync } from "~/libs/fs/mod.ts";
import { FlushFinalizer, Store, StoreRocks } from "~/libs/storage/Store.ts";

const ID = new TupleCodec([U64, new BytesCodec({ size: 2 })]);

type AtomicStores = { readonly [name: string]: Store | StoreRocks };
export type AtomicOptions<T extends AtomicStores> = {
	path: string;
	stores: T;
};

export type InferStores<T extends Atomic<AtomicStores>, TNames extends keyof T["stores"] = keyof T["stores"]> = {
	readonly [K in Extract<keyof T["stores"], TNames>]: T["stores"][K];
};
export type InferBatches<T extends Atomic<AtomicStores>, TNames extends keyof T["stores"] = keyof T["stores"]> = {
	readonly [K in Extract<keyof T["stores"], TNames>]: ReturnType<T["stores"][K]["batch"]>;
};

export class Atomic<T extends AtomicStores> {
	public readonly stores: T;

	private rocksdb: RocksDatabase | undefined;

	private names: readonly string[];
	private rocks: ReadonlyMap<string, StoreRocks>;
	private storeMap: ReadonlyMap<string, Store>;

	private start: Uint8Array | undefined;
	private end: Uint8Array | undefined;

	private startPath: string;
	private endPath: string;

	private constructor(options: AtomicOptions<T>) {
		this.busy = false;
		this.stores = options.stores;

		this.names = Object.keys(this.stores);

		const entries = Object.entries(options.stores);
		this.rocks = new Map(entries.filter((entry): entry is [string, StoreRocks] => entry[1] instanceof StoreRocks));
		this.storeMap = new Map(entries.filter((entry): entry is [string, Store] => entry[1] instanceof Store));

		this.startPath = join(options.path, `start.id`);
		this.endPath = join(options.path, "end.id");
	}

	static open<T extends AtomicStores>(options: AtomicOptions<T>) {
		const self = new Atomic<T>(options);

		for (const kv of self.rocks.values()) {
			self.rocksdb ??= kv.rocksdb;
			if (self.rocksdb !== kv.rocksdb) {
				throw new Error("im sorry but your kv stores has to use the same rocksdb instance, for atomicity");
			}
		}

		Deno.mkdirSync(options.path, { recursive: true });
		self.start = existsSync(self.startPath) ? Deno.readFileSync(self.startPath) : undefined;
		self.end = existsSync(self.endPath) ? Deno.readFileSync(self.endPath) : undefined;

		return self;
	}

	private setStart(id: Uint8Array) {
		using file = Deno.openSync(this.startPath, { create: true, write: true });
		writeFileSync(file, id);
		file.syncSync();
		this.start = id;
	}

	private setEnd(id: Uint8Array) {
		using file = Deno.openSync(this.endPath, { create: true, write: true });
		writeFileSync(file, id);
		file.syncSync();
		this.end = id;
	}

	isConsistent() {
		// Robust against the single-marker state, which is exactly what a crash
		// between setStart and setEnd leaves behind (e.g. a crash during the very
		// first flush, when no end.id exists yet). The previous form fell through to
		// equals(this.start!, this.end!) with one side undefined, and @std/bytes
		// equals reads `.length` on it — throwing TypeError instead of returning
		// false, which then bubbled into recover()'s catch and Deno.exit(1).
		if (!this.start && !this.end) return true;
		if (!this.start || !this.end) return false;
		return equals(this.start, this.end);
	}

	public busy: boolean;

	flush(): void {
		if (this.busy) {
			throw new Error(`im busy man, STOP`);
		}
		if (!this.isConsistent()) {
			throw new Error(`Previous flush state is inconsistent`);
		}
		this.busy = true;
		const flushStart = performance.now();

		try {
			const id = ID.encode([Date.now(), randomBytes(2)]);

			// ===== ATOMIC SNAPSHOT POINT =====
			// Freeze every store synchronously, in one burst with NO await in between,
			// so that no batch apply() — from a tick that keeps running while this
			// flush awaits its disk/rocks I/O — can interleave. Every store therefore
			// captures the exact same logical height. After this point the staged
			// layers are fresh and any concurrent apply is isolated into the next
			// flush.
			//
			// Before this, each store froze at a different instant during the flush:
			// IndexStore at pin(), BlobStore at flush(), KvStore inside the rocks txn,
			// and KvStore had no freeze at all. A block applied between those instants
			// landed in some stores' durable snapshot but not others' — producing
			// cross-store height skew that corrupts on restart (e.g. a tx whose stored
			// spender base points past a shorter on-disk spender array), and KvStore
			// dropped entries applied during its flush outright (lost txid -> later
			// "unresolved prevOut"). The freeze must stay fully synchronous to hold.
			for (const store of this.storeMap.values()) store.freeze();
			for (const store of this.rocks.values()) store.freeze();
			// ===== everything below drains the frozen snapshots; ticks may run =====

			for (const store of this.storeMap.values()) store.pin();

			this.setStart(id);

			for (const store of this.storeMap.values()) store.flush();

			if (this.rocksdb) {
				const finalizers = this.rocksdb.transactionSync((trx) => {
					const finalizers = this.rocks.values().map((store) => store.flush(trx)).toArray();
					trx.putSync("atomic.id", id);
					return finalizers;
				}, { disableSnapshot: true, retryOnBusy: false }) as FlushFinalizer[];
				this.rocksdb.flush();
				if (finalizers) {
					for (const finalizer of finalizers) finalizer();
				}
			}

			// All stores and RocksDB have committed — rollback files are no longer
			// needed. Delete them before writing end.id (best-effort; a leftover
			// rollback file is harmless on next recover since the data is consistent).
			for (const store of this.storeMap.values()) store.finalize();

			this.setEnd(id);
		} catch (reason) {
			console.error("Atomic flush failed:", reason);
			Deno.exit(1);
		} finally {
			this.busy = false;
			console.log(`[flush] flushed ${(performance.now() - flushStart).toFixed(0)}ms`);
		}
	}

	recover(): void {
		if (this.busy) {
			throw new Error("im busy man, STOP");
		}
		try {
			this.busy = true;
			if (this.isConsistent()) return;
			console.log("atomic state is not consistent, recovering...");
			if (this.start && this.rocksdb) {
				const id = this.rocksdb.getSync("atomic.id");
				if (id && equals(id, this.start)) {
					// The rocks transaction committed (it carries atomic.id), and blob
					// stores were flushed before it — so everything is durable. Just mark
					// the end to close the window.
					this.setEnd(id);
					return;
				}
			}
			for (const store of this.storeMap.values()) store.rollback();
			// Re-establish consistency. The interrupted flush wrote start.id but never
			// reached end.id, and we've just undone its on-disk effects; realign the
			// markers. Without this, isConsistent() stays false forever and every
			// subsequent flush() throws "Previous flush state is inconsistent".
			this.setEnd(this.start ?? this.end!);
			console.log("recovered atomic state");
		} catch (reason) {
			console.error(`Atomic recover failed:`, reason);
			Deno.exit(1);
		} finally {
			this.busy = false;
		}
	}

	batch(names: readonly (keyof T)[] = this.names): InferBatches<Atomic<T>> {
		return Object.fromEntries(names.map((name) => [name, this.stores[name]!.batch()])) as never;
	}
}
