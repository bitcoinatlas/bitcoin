import { U48 } from "~/codec/primitives/U48.ts";
import { PARALLELISM_THREADS } from "~/env.ts";
import { chainStorage } from "~/chain/ChainStorage.ts";

/**
 * SpenderIndexer — chain-thread orchestrator for the detached spender index.
 *
 * The heavy work lives in spender.worker.ts; this side just farms out height
 * ranges and advances a persisted, contiguous checkpoint. The whole point
 * (NOTES.md §3) is that indexing runs on ALREADY-COMMITTED data from earlier
 * rounds, in the gap where the chain thread is doing its serial commit work and
 * the extra cores would otherwise idle — so IBD isn't slowed.
 *
 * No write transaction, no batch, no single-writer coupling. Correctness rests
 * on two things instead:
 *   - workers only touch pinned data (bounded reads), and
 *   - the checkpoint is only advanced for a CONTIGUOUS prefix, and only after the
 *     spender column is flushed — so a crash leaves the checkpoint at-or-behind
 *     durable data. On restart we redo from the checkpoint with idempotent puts.
 *
 * `catchUp(target)` is fire-and-forget and non-reentrant: call it every round
 * with the committed tip; if a run is already in flight it just raises the
 * target and returns. Ranges dispatch in strict order but execute in parallel;
 * out-of-order completions are held in `completed` until the frontier reaches
 * them.
 */

const CHECKPOINT_KEY = "spender.height";
// Small tasks keep the frontier moving smoothly and spread load evenly across
// the pool; big enough to amortize the message round-trip.
const HEIGHTS_PER_TASK = 32;
// Flush + checkpoint cadence. Bigger = fewer memtable flushes on the chain
// thread, at the cost of redoing more heights after a crash. Both are cheap.
const PERSIST_EVERY = 2000;
// A handful of workers is enough to soak up the sync-point gap without fighting
// the consume workers for cores during the busy stretches. Leave one core for
// the chain thread + p2p.
const WORKER_COUNT = Math.max(1, Math.min(4, PARALLELISM_THREADS - 1));

type Task = { resolve: () => void };

export class SpenderIndexer {
	private readonly workers: Worker[] = [];
	private readonly tasks: (Task | null)[] = [];
	private readonly idle: number[] = [];
	private readonly ready = Promise.withResolvers<void>();
	private readyCount = 0;

	// frontier: highest height with a contiguous, indexed prefix behind it. Equal
	// to the persisted checkpoint plus whatever we've committed since.
	private frontier: number;
	private persistedFrontier: number;
	private dispatched: number; // next height not yet handed to a worker
	private target = 0;
	private running = false;

	// completed ranges that are done but sit ahead of the frontier (a later range
	// finished before an earlier one). Keyed by `from`, so the frontier can walk.
	private readonly completed = new Map<number, number>();
	private readonly active = new Set<Promise<void>>();

	constructor() {
		const committed = chainStorage.stores.blocks.length();
		const saved = this.readCheckpoint();
		// Never trust a checkpoint that's ahead of committed data: a crash mid-round
		// can roll the block stores back below where the checkpoint reached. Clamp,
		// and re-index forward with idempotent puts.
		this.frontier = Math.min(saved, committed);
		this.persistedFrontier = this.frontier;
		this.dispatched = this.frontier;

		for (let i = 0; i < WORKER_COUNT; i++) {
			const worker = new Worker(new URL("./spender.worker.ts", import.meta.url), { type: "module", name: `spender-${i}` });
			worker.addEventListener("error", (event) => {
				console.error(`[spender] spender-${i} uncaught:`, event.message, event.filename, event.lineno);
				Deno.exit(1);
			});
			worker.addEventListener("message", (event) => this.onMessage(i, event.data));
			this.workers[i] = worker;
			this.tasks[i] = null;
		}

		console.log(`[spender] resuming from height ${this.frontier} (saved=${saved}, committed=${committed}), workers=${WORKER_COUNT}`);
	}

	private readCheckpoint(): number {
		const bytes = chainStorage.rocksdb.getSync(CHECKPOINT_KEY) as Uint8Array | undefined;
		return bytes ? U48.decode(bytes)[0] : 0;
	}

	private onMessage(i: number, data: { type: string; from?: number; to?: number; message?: string; stack?: string }): void {
		if (data.type === "ready") {
			this.idle.push(i);
			if (++this.readyCount === WORKER_COUNT) this.ready.resolve();
			return;
		}
		if (data.type === "index-done") {
			const task = this.tasks[i]!;
			this.tasks[i] = null;
			this.idle.push(i);
			task.resolve();
			return;
		}
		if (data.type === "error") {
			// A double spend or decode failure during IBD of a chain we've already
			// PoW-verified means our own data is corrupt — fail loud, same as the
			// commit path's invariant checks.
			console.error(`[spender] spender-${i} failed on ${data.from}..${data.to}: ${data.message}\n${data.stack ?? ""}`);
			Deno.exit(1);
		}
	}

	private runOn(i: number, from: number, to: number): Promise<void> {
		return new Promise<void>((resolve) => {
			this.tasks[i] = { resolve };
			this.workers[i]!.postMessage({ type: "index", from, to });
		});
	}

	private onCompleted(from: number, to: number): void {
		this.completed.set(from, to);
		// Walk the contiguous prefix: as long as the range starting exactly at the
		// frontier is done, absorb it.
		let next: number | undefined;
		while ((next = this.completed.get(this.frontier)) !== undefined) {
			this.completed.delete(this.frontier);
			this.frontier = next;
		}
		this.persist(false);
	}

	private persist(force: boolean): void {
		if (this.frontier === this.persistedFrontier) return;
		if (!force && this.frontier - this.persistedFrontier < PERSIST_EVERY) return;
		// Make the spender writes durable BEFORE moving the checkpoint. Order is the
		// safety guarantee: crash after the flush but before/around the checkpoint
		// write only ever leaves the checkpoint BEHIND durable data (→ harmless
		// redo), never ahead of it (→ a permanent gap).
		chainStorage.stores.spenders.rocksdb.flushSync();
		chainStorage.rocksdb.putSync(CHECKPOINT_KEY, U48.encode(this.frontier));
		// Also flush the checkpoint itself (disableWAL means an unflushed put is
		// lost on exit). Doesn't affect safety — a lost checkpoint just redoes — it
		// only keeps the redo window small across a clean restart.
		chainStorage.rocksdb.flushSync();
		this.persistedFrontier = this.frontier;
	}

	/**
	 * Index everything committed up to `target` (a block count / tip+1). Returns
	 * immediately if a run is already in progress after raising the target, or if
	 * there's nothing new to do. Never throws to the caller — worker failures
	 * exit the process.
	 */
	async catchUp(target: number): Promise<void> {
		this.target = Math.max(this.target, target);
		if (this.running) return;
		if (this.frontier >= this.target) return;
		this.running = true;
		try {
			await this.ready.promise;
			while (this.frontier < this.target) {
				while (this.idle.length > 0 && this.dispatched < this.target) {
					const i = this.idle.pop()!;
					const from = this.dispatched;
					const to = Math.min(from + HEIGHTS_PER_TASK, this.target);
					this.dispatched = to;
					const p = this.runOn(i, from, to).then(() => this.onCompleted(from, to));
					this.active.add(p);
					p.finally(() => this.active.delete(p));
				}
				if (this.active.size > 0) {
					await Promise.race(this.active);
				} else {
					// idle drained, nothing in flight, but frontier < target: only
					// possible if dispatched already reached target and every range is
					// absorbed — the loop guard will now exit.
					break;
				}
			}
			// Drain any stragglers so the checkpoint reflects the whole prefix.
			while (this.active.size > 0) await Promise.race(this.active);
			this.persist(true);
		} finally {
			this.running = false;
		}
	}
}
