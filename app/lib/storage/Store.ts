import { ArrayCodec, BytesCodec, Codec, Str, U8, UnionCodec } from "@nomadshiba/codec";
import { exists } from "@std/fs";

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
 * `apply()` is replayable — safe to call multiple times; subsequent calls are no-ops
 * if the changes are already applied. This is required for crash recovery.
 *
 * - `id` — UUID that identifies this WAL, used to correlate it with an atomic flush.
 * - `save()` — persist the WAL to disk.
 * - `apply()` — replay the WAL's changes onto the store. Never fails; a failure is a bug and should panic.
 * - `discard()` — delete the WAL file if it exists. Never fails.
 */
export type WAL = {
	id: string;
	apply(): Promise<void>;
	discard(): Promise<void>;
	save(): Promise<void>;
};

/**
 * A persistent store that supports transactional writes and WAL-based crash recovery.
 *
 * - `transaction()` — create an in-memory transaction to stage changes.
 * - `getWAL()` — return the store's existing WAL if one is on disk, or null.
 * - `createWAL()` — create a new WAL from the store's current in-memory state.
 */
export type Store = {
	transaction(): Transaction;
	getWAL(): Promise<WAL | null>;
	createWAL(): Promise<WAL>;
};

const STATE_PATH = "";
const IDS_PATH = "";

const Void = new BytesCodec({ size: 0 });

type State = Codec.InferOutput<typeof State>["kind"];
const State = new UnionCodec({
	"started": Void,
	"saved": Void,
	"applied": Void,
	"discarded": Void,
});

const IDs = new ArrayCodec(Str, { countCodec: U8 });

const atomic = {
	state: {
		async get(): Promise<State> {
			const data = await Deno.readFile(STATE_PATH);
			const [state] = State.decode(data);
			return state["kind"];
		},
		async set(newState: State): Promise<void> {
			const data = State.encode({ kind: newState, value: new Uint8Array() });
			await Deno.writeFile(STATE_PATH, data, { create: true });
		},
	},
	ids: {
		async get(): Promise<string[]> {
			if (!await exists(IDS_PATH)) return [];
			const data = await Deno.readFile(IDS_PATH);
			const [ids] = IDs.decode(data);
			return ids;
		},
		async set(ids: string[]): Promise<void> {
			const data = IDs.encode(ids);
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
export async function atomicFlush(stores: Store[]): Promise<void> {
	if (await atomic.ids.has()) {
		throw new Error("Can't have multiple atomic flushes in progress");
	}

	const wals = await Promise.all(stores.map((s) => s.createWAL()));
	const ids = wals.map((wal) => wal.id);
	await atomic.state.set("started");
	await atomic.ids.set(ids);
	await Promise.all(wals.map((wal) => wal.save()));
	await atomic.state.set("saved");
	await Promise.all(wals.map((wal) => wal.apply()));
	await atomic.state.set("applied");
	await Promise.all(wals.map((wal) => wal.discard()));
	await atomic.state.set("discarded");
	await atomic.ids.delete();
}

/**
 * Recover from a crash by replaying any WALs found on disk.
 *
 * Two cases are handled:
 *
 * 1. **Atomic flush in progress** (IDs file exists): delegate to
 *    `recoverAtomicFlush()` which resumes from the last recorded state.
 *    WALs that belong to the atomic flush are removed from the general pool
 *    so they are not double-applied below.
 *
 * 2. **Orphan WALs** (created outside of `atomicFlush`, e.g. individual store
 *    saves): apply and discard each one independently. These are not atomic
 *    with each other; they are simply replayed in iteration order.
 *
 * Should be called once at startup before any writes.
 */
export async function recover(stores: Store[]): Promise<void> {
	const walsById = new Map<string, WAL>();
	for (const store of stores) {
		const wal = await store.getWAL();
		if (!wal) continue;
		walsById.set(wal.id, wal);
	}

	if (await atomic.ids.has()) {
		const wals: WAL[] = [];
		const ids = await atomic.ids.get();
		for (const id of ids) {
			const wal = walsById.get(id);
			if (wal) wals.push(wal);
			walsById.delete(id);
		}
		await recoverAtomicFlush(wals);
	}

	for (const wal of walsById.values()) {
		await wal.apply();
		await wal.discard();
	}
}

/**
 * Resume an atomic flush that was interrupted by a crash.
 *
 * Reads the last persisted state and continues from there. Because `apply()`
 * is replayable, it is always safe to re-apply WALs that may have already
 * been applied before the crash.
 *
 * State transitions on recovery:
 * - `started`   → WALs may not all be saved; discard all and abort.
 * - `saved`     → WALs are all on disk; apply all, then discard.
 * - `applied`   → Changes are live; just finish discarding WALs.
 * - `discarded` → WALs should all be gone; discard defensively and clean up.
 */
async function recoverAtomicFlush(wals: WAL[]): Promise<void> {
	const state = await atomic.state.get();

	if (state === "started") {
		// Most likely failed while saving WALs. Can't be sure which ones were saved, so just discard all of them.
		for (const wal of wals) {
			await wal.discard();
		}
		await atomic.ids.delete();
		return;
	}

	if (state === "saved") {
		// Most likely failed while applying WALs. Can't be sure which ones were applied, so just apply all of them.
		for (const wal of wals) {
			await wal.apply();
		}
		await atomic.state.set("applied");
		for (const wal of wals) {
			await wal.discard();
		}
		await atomic.state.set("discarded");
		await atomic.ids.delete();
		return;
	}

	if (state === "applied") {
		// Most likely failed while discarding WALs. So just continue discarding all of them.
		for (const wal of wals) {
			await wal.discard();
		}
		await atomic.state.set("discarded");
		await atomic.ids.delete();
		return;
	}

	if (state === "discarded") {
		// All WALs should have been discarded, but just in case, try discarding all of them again and then delete the ids file.
		for (const wal of wals) {
			await wal.discard();
		}
		await atomic.ids.delete();
		return;
	}

	throw new Error(`Invalid atomic flush state: ${state satisfies never}`);
}
