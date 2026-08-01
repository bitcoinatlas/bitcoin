import { join } from "@std/path";
import { Store } from "~/libs/storage/Store.ts";

import { DatabaseSync, type StatementSync } from "node:sqlite";

const BROADCAST_PREFIX = "manifest-store-";

export type ManifestStores = { readonly [name: string]: Store };
export type ManifestOptions<T extends ManifestStores> = {
	path: string;
	stores: T;
};

export class Manifest<T extends ManifestStores> implements Disposable {
	public readonly stores: T;
	public readonly path: string;
	private readonly db: DatabaseSync;
	private readonly storeMap: ReadonlyMap<string, { store: Store; channel: BroadcastChannel }>;
	private readonly pinQuery: StatementSync;
	private readonly getPinsQuery: StatementSync;

	private constructor(options: ManifestOptions<T>) {
		this.path = options.path;
		this.stores = options.stores;
		this.storeMap = new Map(
			Object.entries(this.stores).map(([name, store]) => {
				const channel = new BroadcastChannel(`${BROADCAST_PREFIX}${name}`);
				channel.addEventListener("message", (event) => {
					const size = event.data as number;
					store.reveal(size);
				});
				return [name, { store, channel }];
			}),
		);
		this.db = new DatabaseSync(join(this.path, "db.sqlite"));
		this.db.exec(`PRAGMA journal_mode = WAL;`);
		this.db.exec(`PRAGMA busy_timeout = 5000;`);
		this.db.exec(`CREATE TABLE IF NOT EXISTS pins (name TEXT PRIMARY KEY, size INTEGER NOT NULL);`);
		this.getPinsQuery = this.db.prepare("SELECT name, size FROM pins");
		this.pinQuery = this.db.prepare(
			"INSERT INTO pins (name, size) VALUES (:name, :size) ON CONFLICT(name) DO UPDATE SET size = excluded.size;",
		);
	}

	static open<T extends ManifestStores>(options: ManifestOptions<T>) {
		Deno.mkdirSync(options.path, { recursive: true });
		return new Manifest<T>(options);
	}

	pin(names?: Iterable<keyof T>): void;
	pin(names: Iterable<string> = this.storeMap.keys()) {
		try {
			this.db.exec("BEGIN IMMEDIATE;");
			for (const name of names) {
				const value = this.storeMap.get(name);
				if (!value) throw new Error(`Cannot pin unknown store "${name}".`);
				const { store, channel } = value;
				store.sync();
				const size = store.size();
				this.pinQuery.run({ name, size });
				channel.postMessage(size);
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
			const value = this.storeMap.get(name);
			if (!value) {
				throw new Error(`Pinned store "${name}" does not exist.`);
			}
			const { store } = value;
			const current = store.size();
			if (size > current) store.reveal(size);
			else if (size < current) store.truncate(size);
		}
	}

	close() {
		this.db.close();
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
