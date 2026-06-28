import { randomBytes } from "@noble/hashes/utils";
import { BytesCodec, TupleCodec, U64 } from "@nomadshiba/codec";
import { equals } from "@std/bytes";
import { existsSync } from "@std/fs";
import { join } from "@std/path";
import { writeFileSync } from "~/libs/fs/mod.ts";
import { Store, StoreRocks } from "~/libs/storage/Store.ts";

const ID = new TupleCodec([U64, new BytesCodec({ size: 2 })]);

type AtomicStores = { readonly [name: string]: Store | StoreRocks };
export type AtomicOptions<T extends AtomicStores> = {
	path: string;
	stores: T;
};

export type InferStores<T extends Atomic<AtomicStores>, TNames extends keyof T["stores"] = keyof T["stores"]> = {
	readonly [K in Extract<keyof T["stores"], TNames>]: T["stores"][K];
};

export class Atomic<T extends AtomicStores> {
	public readonly stores: T;

	private rocks: ReadonlyMap<string, StoreRocks>;
	private storeMap: ReadonlyMap<string, Store>;

	private start: Uint8Array | undefined;
	private end: Uint8Array | undefined;

	private startPath: string;
	private endPath: string;

	private constructor(options: AtomicOptions<T>) {
		this.stores = options.stores;

		const entries = Object.entries(options.stores);
		this.rocks = new Map(entries.filter((entry): entry is [string, StoreRocks] => entry[1] instanceof StoreRocks));
		this.storeMap = new Map(entries.filter((entry): entry is [string, Store] => entry[1] instanceof Store));

		this.startPath = join(options.path, `start.id`);
		this.endPath = join(options.path, "end.id");
	}

	static open<T extends AtomicStores>(options: AtomicOptions<T>) {
		const self = new Atomic<T>(options);
		Deno.mkdirSync(options.path, { recursive: true });
		self.start = existsSync(self.startPath) ? Deno.readFileSync(self.startPath) : undefined;
		self.end = existsSync(self.endPath) ? Deno.readFileSync(self.endPath) : undefined;

		return self;
	}

	validPin() {
		if (!this.start && !this.end) return true;
		if (!this.start || !this.end) return false;
		return equals(this.start, this.end);
	}

	pin() {
		try {
			this.setStart();
			for (const store of this.storeMap.values()) store.pin();
		} catch (reason) {
			console.error("pinning failed", reason);
			Deno.exit(1);
		}
	}

	commit() {
		this.setEnd();
	}

	recover(): void {
		if (!this.validPin()) return;
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
		if (!id) {
			throw new Error("did you call end without start, or something? because this is weird");
		}
		using file = Deno.openSync(this.endPath, { create: true, write: true });
		writeFileSync(file, id);
		file.syncSync();
		this.end = id;
	}
}
