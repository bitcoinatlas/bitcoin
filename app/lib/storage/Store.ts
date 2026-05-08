import { ArrayCodec, Codec, Str, StructCodec, U8, UnionCodec, Void } from "@nomadshiba/codec";
import { exists } from "@std/fs";
import { join } from "@std/path";
import { BASE_DATA_DIR } from "~/constants.ts";

/**
 * An in-memory transaction for a store.
 *
 * Enables "all or none" writes: stage changes in memory, then either
 * commit them to disk via a WAL or throw them away entirely.
 *
 * - `apply()` — flush staged changes to disk. Never fails; a failure is a bug and should panic.
 * - `discard()` — throw away staged changes. Like nothing happened.
 */
export type Transaction = {
	apply(): void;
	discard(): void;
};

/**
 * A Write-Ahead Log entry for a single store.
 *
 * Represents a durable record of pending changes that can survive a crash.
 * `apply()` is replayable — safe to call multiple times; This is required for crash recovery.
 *
 * - `save()` — write the WAL to disk. This is a separate step from `apply()` to allow for atomic flushes across multiple stores.
 * - `apply()` — replay the WAL's changes onto the store. Never fails; a failure is a bug and should panic.
 * - `discard()` — delete the WAL file if it exists.
 */
export type WAL = {
	id: string;
	save(): Promise<void>;
	apply(): Promise<void>;
	discard(): Promise<void>;
};

/**
 * A persistent store that supports transactional writes and WAL-based crash recovery.
 *
 * - `name` — unique identifier for the store, used for tracking pending atomic flushes.
 * - `transaction()` — create an in-memory transaction to stage changes.
 * - `WAL()` — return the store's existing WAL if one is on disk, or null.
 * - `WAL({ create: true })` — if WAL doesn't exist return a new one.
 */
export type Store<T extends Transaction = Transaction> = {
	name: string;
	transaction(): T;
	WAL(options: { id: string }): Promise<WAL | null>;
	WAL(options?: { id?: undefined }): Promise<WAL>;
};

const STATE_PATH = join(BASE_DATA_DIR, "atomic", "state.bin");
const IDS_PATH = join(BASE_DATA_DIR, "atomic", "ids.bin");

type State = Codec.InferOutput<typeof State>["kind"];
const State = new UnionCodec({
	"started": Void,
	"saved": Void,
	"applied": Void,
	"discarded": Void,
});

const AtomicWALs = new ArrayCodec(
	new StructCodec({
		store: Str,
		wal: Str,
	}),
	{ countCodec: U8 },
);

const atomicMeta = {
	state: {
		async get(): Promise<State> {
			const data = await Deno.readFile(STATE_PATH);
			const [state] = State.decode(data);
			return state["kind"];
		},
		async set(newState: State): Promise<void> {
			const data = State.encode({ kind: newState, value: null });
			await Deno.writeFile(STATE_PATH, data, { create: true });
		},
	},
	wals: {
		async get(): Promise<Codec.InferOutput<typeof AtomicWALs>> {
			if (!await exists(IDS_PATH)) return [];
			const data = await Deno.readFile(IDS_PATH);
			const [entries] = AtomicWALs.decode(data);
			return entries;
		},
		async set(entries: Codec.InferInput<typeof AtomicWALs>): Promise<void> {
			const data = AtomicWALs.encode(entries);
			await Deno.writeFile(IDS_PATH, data, { create: true });
		},
		async delete(): Promise<void> {
			if (await exists(IDS_PATH)) {
				await Deno.remove(IDS_PATH);
			}
		},
		async has(): Promise<boolean> {
			return await exists(IDS_PATH);
		},
	},
};

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
export async function atomic(stores: Store[]): Promise<void> {
	if (await atomicMeta.wals.has()) {
		throw new Error("Can't have multiple atomic flushes in progress");
	}

	const wals = await Promise.all(stores.map(async (store) => ({ store, wal: await store.WAL() })));
	await atomicMeta.state.set("started");
	await atomicMeta.wals.set(wals.map(({ store, wal }) => ({ store: store.name, wal: wal.id })));
	await Promise.all(wals.map(({ wal }) => wal.save()));
	await atomicMeta.state.set("saved");
	await Promise.all(wals.map(({ wal }) => wal.apply()));
	await atomicMeta.state.set("applied");
	await Promise.all(wals.map(({ wal }) => wal.discard()));
	await atomicMeta.state.set("discarded");
	await atomicMeta.wals.delete();
}

/**
 * Recover from a crash by replaying any WALs found on disk.
 * Should be called once at startup before any writes.
 */
export async function recover(stores: Store[]): Promise<void> {
	if (!await atomicMeta.wals.has()) {
		return;
	}
	const state = await atomicMeta.state.get();

	const storesByName = new Map(stores.map((store) => [store.name, store]));
	const atomicWALs = await atomicMeta.wals.get();
	const wals: WAL[] = [];
	for (const entry of atomicWALs) {
		const store = storesByName.get(entry.store);
		if (!store) {
			throw new Error(`Store ${entry.store} not found for atomic WAL recovery`);
		}
		const wal = await store.WAL({ id: entry.wal });
		if (!wal) {
			if (state === "applied") continue;
			if (state === "discarded") continue;
			throw new Error(`WAL ${entry.wal} for store ${entry.store} not found during atomic WAL recovery`);
		}
		wals.push(wal);
	}

	if (state === "started") {
		// Most likely failed while saving WALs. Can't be sure which ones were saved, so just discard all of them.
		for (const wal of wals) {
			await wal.discard();
		}
		await atomicMeta.wals.delete();
		return;
	}

	if (state === "saved") {
		// Most likely failed while applying WALs. Can't be sure which ones were applied, so just apply all of them.
		for (const wal of wals) {
			await wal.apply();
		}
		await atomicMeta.state.set("applied");
		for (const wal of wals) {
			await wal.discard();
		}
		await atomicMeta.state.set("discarded");
		await atomicMeta.wals.delete();
		return;
	}

	if (state === "applied") {
		// Most likely failed while discarding WALs. So just continue discarding all of them.
		for (const wal of wals) {
			await wal.discard();
		}
		await atomicMeta.state.set("discarded");
		await atomicMeta.wals.delete();
		return;
	}

	if (state === "discarded") {
		// All WALs should have been discarded, but just in case, try discarding all of them again and then delete the ids file.
		for (const wal of wals) {
			await wal.discard();
		}
		await atomicMeta.wals.delete();
		return;
	}

	throw new Error(`Invalid atomic flush state: ${state satisfies never}`);
}


