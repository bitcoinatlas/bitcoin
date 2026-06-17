import { RocksDatabase } from "@harperfast/rocksdb-js";
import { BytesCodec, TupleCodec, U64 } from "@nomadshiba/codec";
import { equals } from "@std/bytes";
import { exists } from "@std/fs";
import { join } from "@std/path";
import { randomBytes } from "@noble/hashes/utils";
import { Store, StoreRocks } from "~/storage/Store.ts";
import { writeFile } from "~/utils/fs.ts";

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

	private _rocksdb: RocksDatabase | undefined;

	private _names: readonly string[];
	private _rocks: ReadonlyMap<string, StoreRocks>;
	private _stores: ReadonlyMap<string, Store>;

	private _start: Uint8Array | undefined;
	private _end: Uint8Array | undefined;

	private _startPath: string;
	private _endPath: string;

	private constructor(options: AtomicOptions<T>) {
		this.busy = false;
		this.stores = options.stores;

		this._names = Object.keys(this.stores);

		const entries = Object.entries(options.stores);
		this._rocks = new Map(entries.filter((entry): entry is [string, StoreRocks] => entry[1] instanceof StoreRocks));
		this._stores = new Map(entries.filter((entry): entry is [string, Store] => entry[1] instanceof Store));

		this._startPath = join(options.path, `start.id`);
		this._endPath = join(options.path, "end.id");
	}

	static async open<T extends AtomicStores>(options: AtomicOptions<T>) {
		const self = new Atomic<T>(options);

		for (const kv of self._rocks.values()) {
			self._rocksdb ??= kv.rocksdb;
			if (self._rocksdb !== kv.rocksdb) {
				throw new Error("im sorry but your kv stores has to use the same rocksdb instance, for atomicity");
			}
		}

		await Deno.mkdir(options.path, { recursive: true });
		self._start = await exists(self._startPath) ? await Deno.readFile(self._startPath) : undefined;
		self._end = await exists(self._endPath) ? await Deno.readFile(self._endPath) : undefined;

		return self;
	}

	private async _setStart(id: Uint8Array) {
		using file = await Deno.open(this._startPath, { create: true, write: true });
		await writeFile(file, id);
		await file.sync();
		this._start = id;
	}

	private async _setEnd(id: Uint8Array) {
		using file = await Deno.open(this._endPath, { create: true, write: true });
		await writeFile(file, id);
		await file.sync();
		this._end = id;
	}

	isConsistent() {
		return (!this._start && !this._end) || equals(this._start!, this._end!);
	}

	public busy: boolean;

	async flush(): Promise<void> {
		if (this.busy) {
			throw new Error(`im busy man, STOP`);
		}
		if (!this.isConsistent()) {
			throw new Error(`Previous flush state is inconsistent`);
		}
		this.busy = true;
		try {
			const id = ID.encode([Date.now(), randomBytes(2)]);
			await Promise.all(this._stores.values().map((store) => store.pin()));
			await this._setStart(id);
			await Promise.all(this._stores.values().map((store) => store.flush()));

			if (this._rocksdb) {
				const finalizers = await this._rocksdb.transaction(async (trx) => {
					const finalizers = await Promise.all(this._rocks.values().map((store) => store.flush(trx)));
					await trx.put("atomic.id", id);
					return finalizers;
				});
				await this._rocksdb.flush();
				if (finalizers) {
					for (const finalizer of finalizers) {
						finalizer();
					}
				}
			}

			await this._setEnd(id);
		} catch (reason) {
			console.error("Atomic flush failed:", reason);
			Deno.exit(1);
		} finally {
			this.busy = false;
		}
	}

	async recover(): Promise<void> {
		if (this.busy) {
			throw new Error("im busy man, STOP");
		}
		try {
			this.busy = true;
			if (this.isConsistent()) return;
			if (this._start && this._rocksdb) {
				const id = await this._rocksdb.get("atomic.id");
				if (id && equals(id, this._start)) {
					await this._setEnd(id);
					return;
				}
			}
			await Promise.all(this._stores.values().map((store) => store.rollback()));
			if (this._start) await this._setEnd(this._start);
		} catch (reason) {
			console.error(`Atomic recover failed:`, reason);
			Deno.exit(1);
		} finally {
			this.busy = false;
		}
	}

	batch(names: readonly (keyof T)[] = this._names): InferBatches<Atomic<T>> {
		return Object.fromEntries(names.map((name) => [name, this.stores[name]!.batch()])) as never;
	}
}
