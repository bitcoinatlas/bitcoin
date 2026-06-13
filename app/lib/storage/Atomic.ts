import { Codec, EnumCodec, Void } from "@nomadshiba/codec";
import { join } from "@std/path";
import { Store, WAL } from "~/lib/storage/Store.ts";

type AtomicState = Codec.InferOutput<typeof AtomicState>["kind"];
const AtomicState = new EnumCodec({
	"started": Void,
	"saved": Void,
	"applied": Void,
	"discarded": Void,
});

type AtomicStores = { readonly [name: string]: Store };
export type AtomicOptions<T extends AtomicStores> = {
	path: string;
	stores: T;
};

export class Atomic<T extends AtomicStores> {
	public readonly statePath: string;
	public readonly storeNamesPath: string;
	public readonly stores: T;
	public readonly storeEntries: readonly (readonly [string, Store])[];

	private constructor(path: string, stores: T) {
		this.statePath = join(path, "state.bin");
		this.storeNamesPath = join(path, "stores.bin");
		this.stores = stores;
		this.storeEntries = Object.entries(stores);
	}

	static async open<T extends AtomicStores>(options: AtomicOptions<T>) {
		await Deno.mkdir(options.path, { recursive: true });
		const self = new Atomic<T>(options.path, options.stores);
		await self.setState("discarded");
		return self;
	}

	async getState(): Promise<AtomicState> {
		const data = await Deno.readFile(this.statePath);
		const [state] = AtomicState.decode(data);
		return state["kind"];
	}

	async setState(newState: AtomicState): Promise<void> {
		const data = AtomicState.encode({ kind: newState, value: null });
		await Deno.writeFile(this.statePath, data, { create: true });
	}

	/**
	 * Flush multiple stores atomically: either all changes land on disk or none do.
	 *
	 * The IDs file acts as the "in-progress" sentinel. Its presence means an atomic
	 * flush is underway; its absence means we are clean. Because of this, state is
	 * written before IDs: if we crash between the two, no IDs file exists and
	 * `recover()` treats it as a no-op for the atomic path.
	 *
	 * Progress is tracked through a state machine persisted to disk so that
	 * `recover()` can resume from wherever we crashed:
	 *
	 *   started  → all WALs created, IDs written, saving WALs in progress
	 *   saved    → all WALs on disk, applying in progress
	 *   applied  → all changes live, discarding WALs in progress
	 *   discarded → all WALs deleted, deleting IDs file in progress
	 *
	 * Throws if another atomic flush is already in progress (IDs file exists).
	 * Call `recover()` first to clear a previous crashed flush before retrying.
	 */
	async flush(): Promise<void> {
		if (await this.getState() !== "discarded") {
			throw new Error("Can't have multiple atomic flushes in progress");
		}

		await this.setState("started");
		const wals = await Promise.all(this.storeEntries.map(([, store]) => store.createWAL()));
		await this.setState("saved");
		await Promise.all(wals.map((wal) => wal.apply()));
		await this.setState("applied");
		await Promise.all(wals.map((wal) => wal.discard()));
		await this.setState("discarded");
	}

	/**
	 * Recover from a crash by replaying any WALs found on disk.
	 * Should be called once at startup before any writes.
	 */
	async recover(): Promise<void> {
		const state = await this.getState();
		if (state === "discarded") return;

		const wals: WAL[] = [];
		for (const [name, store] of this.storeEntries) {
			const { wal } = store;
			if (!wal) {
				if (state === "started") continue;
				if (state === "applied") continue;
				throw new Error(`WAL for store ${name} not found during atomic WAL recovery`);
			}
			wals.push(wal);
		}

		if (state === "started") {
			// Most likely failed while saving WALs. Can't be sure which ones were saved, so just discard all of them.
			for (const wal of wals) {
				await wal.discard();
			}
			return;
		}

		if (state === "saved") {
			// Most likely failed while applying WALs. Can't be sure which ones were applied, so just apply all of them.
			for (const wal of wals) {
				await wal.apply();
			}
			await this.setState("applied");
			for (const wal of wals) {
				await wal.discard();
			}
			await this.setState("discarded");
			return;
		}

		if (state === "applied") {
			// Most likely failed while discarding WALs. So just continue discarding all of them.
			for (const wal of wals) {
				await wal.discard();
			}
			await this.setState("discarded");
			return;
		}

		if (state === "discarded") {
			// All WALs should have been discarded, but just in case, try discarding all of them again and then delete the ids file.
			for (const wal of wals) {
				await wal.discard();
			}
			return;
		}

		throw new Error(`Invalid atomic flush state: ${state satisfies never}`);
	}
}
