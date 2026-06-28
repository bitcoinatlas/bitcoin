import { randomBytes } from "@noble/hashes/utils";
import { BytesCodec, TupleCodec, U64 } from "@nomadshiba/codec";
import { equals } from "@std/bytes";
import { existsSync } from "@std/fs";
import { join } from "@std/path";
import { writeFileSync } from "~/libs/fs/mod.ts";
import { Store, StoreRocks } from "~/libs/storage/Store.ts";
import { RocksDatabase, Transaction } from "@harperfast/rocksdb-js";

const ID = new TupleCodec([U64, new BytesCodec({ size: 2 })]);

export type AtomicStores = { readonly [name: string]: Store | StoreRocks };
export type AtomicOptions<T extends AtomicStores> = {
	path: string;
	rocksdb: RocksDatabase;
	stores: T;
};

export class Atomic<T extends AtomicStores> {
	public readonly stores: T;
	private readonly rocksdb: RocksDatabase;

	private rockMap: ReadonlyMap<string, StoreRocks>;
	private storeMap: ReadonlyMap<string, Store>;

	private start: Uint8Array;
	private end: Uint8Array;

	private startPath: string;
	private endPath: string;

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

		this.startPath = join(options.path, `start.id`);
		this.endPath = join(options.path, "end.id");
		this.start = new Uint8Array();
		this.end = new Uint8Array();
	}

	static open<T extends AtomicStores>(options: AtomicOptions<T>) {
		const self = new Atomic<T>(options);
		Deno.mkdirSync(options.path, { recursive: true });
		if (existsSync(self.startPath)) self.start = Deno.readFileSync(self.startPath);
		if (existsSync(self.endPath)) self.end = Deno.readFileSync(self.endPath);

		return self;
	}

	pin() {
		try {
			this.setStart();
			for (const store of this.storeMap.values()) store.pin();
			this.setEnd();
		} catch (reason) {
			console.error("pinning failed", reason);
			Deno.exit(1);
		}
	}

	trx(call: (stores: T, trx: Transaction) => void) {
		try {
			this.pin();
			this.rocksdb.transactionSync((trx) => call(this.stores, trx));
			this.rocksdb.flushSync();
		} catch (reason) {
			console.error("atomic trx failed", reason);
			Deno.exit(1);
		}
	}

	recover(): void {
		if (!equals(this.start, this.end)) return;
		for (const store of this.storeMap.values()) store.rollback();
	}

	private setStart() {
		const id = ID.encode([Date.now(), randomBytes(2)]);
		using file = Deno.openSync(this.startPath, { create: true, write: true });
		writeFileSync(file, id);
		file.syncSync();
		this.start = id;
	}

	private setEnd() {
		const id = this.start;
		using file = Deno.openSync(this.endPath, { create: true, write: true });
		writeFileSync(file, id);
		file.syncSync();
		this.end = id;
	}
}
