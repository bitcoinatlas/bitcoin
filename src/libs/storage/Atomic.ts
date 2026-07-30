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
		// Multiple workers open the SAME db.sqlite (p2p pins the header domain,
		// chain pins the block/index domain). IF NOT EXISTS so the second opener
		// doesn't throw, and busy_timeout so their BEGIN IMMEDIATE transactions
		// wait for each other instead of failing with SQLITE_BUSY.
		this.db.execute(`CREATE TABLE IF NOT EXISTS pins (name TEXT PRIMARY KEY, size INTEGER NOT NULL);`);
		this.db.execute(`PRAGMA busy_timeout = 5000;`);
		this.getPinsQuery = this.db.prepareQuery("SELECT name, size FROM pins");
		this.pinQuery = this.db.prepareQuery(
			"INSERT INTO pins (name, size) VALUES (?,?) ON CONFLICT(name) DO UPDATE SET size = excluded.size;",
		);
	}

	static open<T extends AtomicStores>(options: AtomicOptions<T>) {
		Deno.mkdirSync(options.path, { recursive: true });
		return new Atomic<T>(options);
	}

	/**
	 * Commit a consistent size snapshot for a set of stores. Pass `names` to pin
	 * only a subset — this is what lets two workers each own a disjoint domain of
	 * the same Atomic without stepping on each other: each pins ONLY the stores
	 * it writes, so it never records a torn size for a store another worker is
	 * mid-writing. With no argument, pins every store (single-writer / recovery
	 * setup).
	 */
	pin(names?: readonly string[]) {
		const targets: [string, Store][] = [];
		if (names) {
			for (const name of names) {
				const store = this.storeMap.get(name);
				if (!store) throw new Error(`Cannot pin unknown store "${name}".`);
				targets.push([name, store]);
			}
		} else {
			for (const entry of this.storeMap) targets.push(entry);
		}

		try {
			this.db.execute("BEGIN IMMEDIATE;");
			for (const [name, store] of targets) {
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
