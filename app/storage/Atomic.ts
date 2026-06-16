import { exists } from "@std/fs";
import { join } from "@std/path";
import { equals } from "@std/bytes";
import { Store } from "~/storage/Store.ts";
import { writeFile } from "~/utils/fs.ts";
import { randomBytes } from "@noble/hashes/utils";

type AtomicStores = { readonly [name: string]: Store };
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
	public readonly statePath: string;
	public readonly stores: T;
	public readonly storeMap: ReadonlyMap<string, Store>;
	private _start: Uint8Array | undefined;
	private _end: Uint8Array | undefined;

	private _startPath: string;
	private _endPath: string;

	private _multiBatchObjectFactory: Function;
	private constructor(path: string, stores: T) {
		this.statePath = join(path, "state.bin");
		this.stores = stores;
		this.storeMap = new Map(Object.entries(stores));
		this.busy = false;
		this._startPath = join(path, `start.id`);
		this._endPath = join(path, "end.id");

		// V8 class/struct
		this._multiBatchObjectFactory = new Function(
			...this.storeMap.keys().map((_, i) => `arg${i}`),
			`return{${this.storeMap.keys().map((field, i) => `${JSON.stringify(field)}:arg${i}`).toArray().join(",")}}`,
		);
	}

	static async open<T extends AtomicStores>(options: AtomicOptions<T>) {
		await Deno.mkdir(options.path, { recursive: true });
		const self = new Atomic<T>(options.path, options.stores);

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
			const id = randomBytes(32);
			await this._setStart(id);
			await Promise.all(this.storeMap.values().map((store) => store.flush()));
			await this._setEnd(id);
		} catch (reason) {
			console.error("Atomic flush failed:", reason);
			Deno.exit(1);
		} finally {
			this.busy = false;
		}
	}

	async recover(): Promise<void> {
		if (this.isConsistent()) return;
		if (this.busy) {
			throw new Error("im busy man, STOP");
		}
		this.busy = true;
		try {
			await Promise.all(this.storeMap.values().map((store) => store.rollback()));
		} catch (reason) {
			console.error(`Atomic rollback failed:`, reason);
			Deno.exit(1);
		} finally {
			this.busy = false;
		}
	}

	batch(names?: readonly (keyof T)[]): InferBatches<Atomic<T>> {
		if (!names) return this._multiBatchObjectFactory(...this.storeMap.values().map((store) => store.batch()));
		return Object.fromEntries(names.map((name) => [name, this.stores[name]!.batch()])) as never;
	}
}
