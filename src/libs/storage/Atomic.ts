import { DatabaseSync, type StatementSync } from "node:sqlite";
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
	private readonly db: DatabaseSync;
	private readonly storeMap: ReadonlyMap<string, Store>;
	private readonly pinQuery: StatementSync;
	private readonly getPinsQuery: StatementSync;

	private constructor(options: AtomicOptions<T>) {
		this.path = options.path;
		this.stores = options.stores;
		this.storeMap = new Map(Object.entries(this.stores));
		this.db = new DatabaseSync(join(this.path, "db.sqlite"));
		// Multiple workers open the SAME db.sqlite (p2p pins the header domain,
		// chain pins the block/index domain) as separate connections in the same
		// process. WAL is REQUIRED here: without it, a completed read on one
		// connection deadlocks a BEGIN IMMEDIATE/COMMIT on the other, and
		// busy_timeout can't break an intra-process lock cycle. node:sqlite is the
		// native build (the WASM build can't enable WAL); WAL lets readers and a
		// single writer run concurrently without blocking. IF NOT EXISTS so the
		// second opener doesn't throw; busy_timeout still guards the rare
		// writer-vs-writer overlap across the two disjoint domains.
		this.db.exec(`PRAGMA journal_mode = WAL;`);
		this.db.exec(`PRAGMA busy_timeout = 5000;`);
		this.db.exec(`CREATE TABLE IF NOT EXISTS pins (name TEXT PRIMARY KEY, size INTEGER NOT NULL);`);
		this.getPinsQuery = this.db.prepare("SELECT name, size FROM pins");
		this.pinQuery = this.db.prepare(
			"INSERT INTO pins (name, size) VALUES (:name, :size) ON CONFLICT(name) DO UPDATE SET size = excluded.size;",
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
	pin(names?: readonly (keyof T)[]): void;
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
			this.db.exec("BEGIN IMMEDIATE;");
			for (const [name, store] of targets) {
				store.sync();
				this.pinQuery.run({ name, size: store.size() });
			}
			this.db.exec("COMMIT;");
		} catch (reason) {
			this.db.exec("ROLLBACK;");
			throw reason;
		}
	}

	rollback(): void {
		const pins = this.getPinsQuery.all() as { name: string; size: number }[];
		for (const { name, size } of pins) {
			const store = this.storeMap.get(name);
			if (!store) {
				throw new Error(`Pinned store "${name}" does not exist.`);
			}
			store.resize(size);
		}
	}

	close() {
		this.db.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
