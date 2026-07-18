import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";
import { Store, StoreAppendOnly } from "~/libs/storage/Store.ts";

export type AtomicStores = { readonly [name: string]: Store };
export type AtomicOptions<T extends AtomicStores> = {
	rocksdb: RocksDatabase;
	stores: T;
};

export class Atomic<T extends AtomicStores> {
	public readonly stores: T;
	private readonly rocksdb: RocksDatabase;

	private appendOnlyStores: ReadonlyMap<string, StoreAppendOnly>;

	private constructor(options: AtomicOptions<T>) {
		this.rocksdb = options.rocksdb;
		this.stores = options.stores;

		const appendOnlyStores = new Map<string, StoreAppendOnly>();
		for (const [name, store] of Object.entries(options.stores)) {
			if (this.rocksdb.path !== store.rocksdb.path) {
				throw new Error([
					"inconsistent rocksdb paths.",
					"for state consistency use different rocksdb columns, not different rocksdb paths",
				].join("\n"));
			}
			if (store instanceof StoreAppendOnly) {
				appendOnlyStores.set(name, store);
			}
		}
		this.appendOnlyStores = appendOnlyStores;
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
				for (const store of this.appendOnlyStores.values()) {
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
		for (const store of this.appendOnlyStores.values()) store.rollback(transaction);
	}
}
