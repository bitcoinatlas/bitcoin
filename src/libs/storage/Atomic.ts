import { DB, PreparedQuery } from "@pomdtr/sqlite";
import { join } from "@std/path";
import { Store } from "~/libs/storage/Store.ts";

export type AtomicStores = { readonly [name: string]: Store };
export type AtomicOptions<T extends AtomicStores> = {
	path: string;
	stores: T;
};

export class Atomic<T extends AtomicStores> implements Disposable {
	public readonly stores: T;
	public readonly path: string;
	private readonly db: DB;
	private readonly storeMap: ReadonlyMap<string, Store>;
	private readonly pinQuery: PreparedQuery<never, never, { name: string; size: number }>;
	private readonly getPinsQuery: PreparedQuery<[string, number]>;

	private constructor(options: AtomicOptions<T>) {
		this.path = options.path;
		this.stores = options.stores;
		this.storeMap = new Map(Object.entries(this.stores));
		this.db = new DB(join(this.path, "db.sqlite"), { mode: "create" });
		this.db.execute(`CREATE TABLE pins (name TEXT PRIMARY KEY, size INTEGER NOT NULL);`);
		this.getPinsQuery = this.db.prepareQuery("SELECT name, size FROM pins");
		this.pinQuery = this.db.prepareQuery(
			"INSERT INTO pins (name, size) VALUES (?,?) ON CONFLICT(name) DO UPDATE SET size = excluded.size;",
		);
	}

	static open<T extends AtomicStores>(options: AtomicOptions<T>) {
		return new Atomic<T>(options);
	}

	pin() {
		try {
			this.db.execute("BEGIN IMMEDIATE;");
			for (const [name, store] of this.storeMap) {
				store.sync();
				this.pinQuery.execute({ name, size: store.size() });
			}
			this.db.execute("COMMIT;");
		} catch (reason) {
			this.db.execute("ROLLBACK;");
			throw reason;
		}
	}

	rollback(): void {
		const pins = this.getPinsQuery.all();
		for (const [name, size] of pins) {
			const store = this.storeMap.get(name);
			if (!store) {
				throw new Error(`Pinned store "${name}" does not exist.`);
			}
			store.resize(size);
		}
	}

	close() {
		this.pinQuery.finalize();
		this.getPinsQuery.finalize();
		this.db.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
