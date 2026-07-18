import type { Job, Result } from "./compress.worker.ts";

type Pending = {
	job: Job;
	resolve: (compressedSize: number) => void;
	reject: (error: Error) => void;
};

/**
 * A persistent pool of Worker threads that run zstd compression on real OS
 * threads (Deno's node:zlib runs compression on the calling thread, so this is
 * the only way to get genuine multicore compression — see compress.worker.ts).
 *
 * Workers are spawned once and reused for the pool's lifetime. Jobs are queued
 * and handed to whichever worker goes idle next, so at most `size` chunks are
 * compressed concurrently. Only the zstd + file I/O runs in the workers; the
 * caller keeps all the rename/remove/reader bookkeeping on the main thread.
 */
export class CompressWorkerPool {
	private readonly workers: Worker[] = [];
	private readonly idle: Worker[] = [];
	private readonly busy = new Map<Worker, Pending>();
	private readonly queue: Pending[] = [];
	private nextJobId = 0;
	private disposed = false;

	constructor(size: number) {
		const count = Math.max(1, size);
		for (let i = 0; i < count; i++) {
			const worker = new Worker(new URL("./compress.worker.ts", import.meta.url), {
				type: "module",
				name: `compress-${i}`,
			});
			worker.onmessage = (event: MessageEvent<Result>) => this.onWorkerDone(worker, event.data);
			worker.onerror = (event) => this.onWorkerError(worker, event);
			this.workers.push(worker);
			this.idle.push(worker);
		}
	}

	/** Compress rawPath into tmpPath with the given zstd params. Resolves with the compressed byte size. */
	compress(index: number, rawPath: string, tmpPath: string, params: Record<number, number>): Promise<number> {
		if (this.disposed) return Promise.reject(new Error("compression pool is disposed"));
		return new Promise<number>((resolve, reject) => {
			const job: Job = { id: this.nextJobId++, index, rawPath, tmpPath, params };
			const pending: Pending = { job, resolve, reject };
			const worker = this.idle.pop();
			if (worker) {
				this.assign(worker, pending);
			} else {
				this.queue.push(pending);
			}
		});
	}

	private assign(worker: Worker, pending: Pending) {
		this.busy.set(worker, pending);
		console.log(`[compress] chunk ${pending.job.index} starting`);
		worker.postMessage(pending.job);
	}

	private onWorkerDone(worker: Worker, result: Result) {
		const pending = this.busy.get(worker);
		this.busy.delete(worker);
		if (pending) {
			if (result.ok) pending.resolve(result.compressedSize);
			else pending.reject(new Error(`worker failed to compress chunk ${result.index}: ${result.error}`));
		}
		if (this.disposed) {
			worker.terminate();
			return;
		}
		const next = this.queue.shift();
		if (next) this.assign(worker, next);
		else this.idle.push(worker);
	}

	private onWorkerError(worker: Worker, event: ErrorEvent) {
		// A crashed worker rejects whatever it was running; the worker is dead, so
		// don't hand it more work. The pool shrinks by one — acceptable for a
		// background maintenance task, and it'll be gone entirely on close().
		const pending = this.busy.get(worker);
		this.busy.delete(worker);
		event.preventDefault?.();
		if (pending) pending.reject(new Error(`compression worker crashed: ${event.message}`));
		const idleAt = this.idle.indexOf(worker);
		if (idleAt >= 0) this.idle.splice(idleAt, 1);
		worker.terminate();
	}

	dispose() {
		this.disposed = true;
		// Reject anything still queued (never started).
		for (const pending of this.queue) pending.reject(new Error("compression pool disposed before job ran"));
		this.queue.length = 0;
		// Idle workers can go now; busy ones terminate when their current job posts back.
		for (const worker of this.idle) worker.terminate();
		this.idle.length = 0;
	}
}
