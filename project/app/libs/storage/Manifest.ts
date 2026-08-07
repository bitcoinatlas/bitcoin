import { join } from "@std/path";
import { Store } from "~/libs/storage/Store.ts";

import { DatabaseSync, type StatementSync } from "node:sqlite";

const BROADCAST_PREFIX = "manifest-store-";

export type ManifestStores = { readonly [name: string]: Store };
export type ManifestOptions<T extends ManifestStores> = {
	path: string;
	stores: T;
	pinner: boolean;
};

export class Manifest<T extends ManifestStores> implements Disposable {
	public readonly stores: T;
	public readonly path: string;
	private readonly db: DatabaseSync;
	private readonly storeMap: ReadonlyMap<string, { store: Store; channel: BroadcastChannel }>;
	private readonly pinQuery: StatementSync;
	private readonly revealQuery: StatementSync;
	private readonly getSizesQuery: StatementSync;

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
		this.db = new DatabaseSync(join(this.path, "manifest.sqlite"));
		this.db.exec(`PRAGMA journal_mode = WAL;`);
		this.db.exec(`PRAGMA busy_timeout = 5000;`);
		this.db.exec(
			`CREATE TABLE IF NOT EXISTS sizes (name TEXT PRIMARY KEY, pin INTEGER NOT NULL DEFAULT 0, reveal INTEGER NOT NULL DEFAULT 0);`,
		);
		this.getSizesQuery = this.db.prepare("SELECT * FROM sizes");
		this.pinQuery = this.db.prepare(
			`INSERT INTO sizes (name, pin) VALUES (:name, :size) ON CONFLICT(name) DO UPDATE SET pin = excluded.pin;`,
		);
		this.revealQuery = this.db.prepare(
			`INSERT INTO sizes (name, reveal) VALUES (:name, :size) ON CONFLICT(name) DO UPDATE SET reveal = excluded.reveal;`,
		);
	}

	public static open<T extends ManifestStores>(options: ManifestOptions<T>) {
		Deno.mkdirSync(options.path, { recursive: true });
		const manifest = new Manifest<T>(options);
		const pins = manifest.getSizesQuery.all() as { name: string; pin: number; reveal: number }[];
		if (options.pinner) {
			for (const { name, pin, reveal } of pins) {
				const value = manifest.storeMap.get(name);
				if (!value) throw new Error(`Pinned store "${name}" does not exist.`);
				const { store } = value;
				store.reveal(reveal);
				if (pin === reveal) continue;
				store.truncate(pin);
			}
		} else {
			for (const { name, pin } of pins) {
				const value = manifest.storeMap.get(name);
				if (!value) throw new Error(`Pinned store "${name}" does not exist.`);
				const { store } = value;
				store.reveal(pin);
			}
		}
		return manifest;
	}

	public pin() {
		try {
			this.db.exec("BEGIN IMMEDIATE;");
			for (const [name, { store }] of this.storeMap) {
				this.revealQuery.run({ name, size: store.size() });
			}
			this.db.exec("COMMIT;");
		} catch (reason) {
			console.error(reason);
			if (this.db.isTransaction) this.db.exec("ROLLBACK;");
			Deno.kill(Deno.pid);
		}
		try {
			this.db.exec("BEGIN IMMEDIATE;");
			for (const [name, { store, channel }] of this.storeMap) {
				store.sync();
				const size = store.size();
				this.pinQuery.run({ name, size });
				channel.postMessage(size);
			}
			this.db.exec("COMMIT;");
		} catch (reason) {
			console.error(reason);
			if (this.db.isTransaction) this.db.exec("ROLLBACK;");
			Deno.kill(Deno.pid);
		}
	}

	public close() {
		this.db.close();
	}

	public [Symbol.dispose](): void {
		this.close();
	}
}
