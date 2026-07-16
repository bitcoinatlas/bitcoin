import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";
import { Store, StoreRocks } from "~/libs/storage/Store.ts";

export type AtomicStores = { readonly [name: string]: Store | StoreRocks };
export type AtomicOptions<T extends AtomicStores> = {
	rocksdb: RocksDatabase;
	stores: T;
};

export class Atomic<T extends AtomicStores> {
	public readonly stores: T;
	private readonly rocksdb: RocksDatabase;

	private rockMap: ReadonlyMap<string, StoreRocks>;
	private storeMap: ReadonlyMap<string, Store>;

	private constructor(options: AtomicOptions<T>) {
		this.rocksdb = options.rocksdb;
		this.stores = options.stores;

		const entries = Object.entries(options.stores);
		this.rockMap = new Map(entries.filter((entry): entry is [string, StoreRocks] => entry[1] instanceof StoreRocks));
		this.storeMap = new Map(entries.filter((entry): entry is [string, Store] => entry[1] instanceof Store));

		for (const rock of this.rockMap.values()) {
			if (this.rocksdb.path === rock.rocksdb.path) continue;
			throw new Error("inconsistent rocksdb paths");
		}
	}

	static open<T extends AtomicStores>(options: AtomicOptions<T>) {
		return new Atomic<T>(options);
	}

	private inTrx = false;

	async trx(call: (stores: T, transaction: Transaction) => Promise<void> | void) {
		if (this.inTrx) throw new Error("atomic.trx called re-entrantly — a caller forgot to await");
		this.inTrx = true;
		try {
			await this.rocksdb.transaction((trx) => {
				call(this.stores, trx);
				for (const store of this.storeMap.values()) {
					store.pin(trx);
				}
			});
		} catch (reason) {
			console.error("atomic trx failed", reason);
			Deno.exit(1);
		} finally {
			this.inTrx = false;
		}
	}

	recover(transaction?: Transaction): void {
		for (const store of this.storeMap.values()) store.rollback(transaction);
	}
}
