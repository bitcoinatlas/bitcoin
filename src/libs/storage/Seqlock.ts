import { Mmap } from "@nomadshiba/mmap";
import { getStorageControl } from "~/libs/storage/control.ts";
import type { SharedControl } from "~/libs/storage/SharedControl.ts";

/**
 * A single-writer / many-reader seqlock. The **payload** lives in a
 * memory-mapped file (persisted, shared across workers via the page cache). The
 * **version word** lives in the process {@link SharedControl} block — because
 * `Atomics` only emit a real cross-isolate fence on a `SharedArrayBuffer`, not
 * on the non-shared `ArrayBuffer` that wraps an mmap pointer. That fence is what
 * orders the payload copy on both sides, so a reader never sees a torn payload,
 * never blocks the writer, and gets correct visibility on weak (ARM) hardware
 * too.
 *
 * The version is bumped ODD before a write and EVEN after it. A reader snapshots
 * the version, copies the payload, then re-reads the version: if it changed or
 * was odd, retry.
 *
 * The version lives in the (ephemeral) control block, fresh and even every
 * process, so a crash can never leave a persisted odd version to inherit — the
 * old blind-`add` parity bug is gone by construction. Only the payload persists;
 * a torn payload from a mid-write crash is healed by the writer republishing a
 * reconstructed-good value at boot, before any reader is racing.
 *
 * Boot window: until the control block is set in this isolate, reads/writes go
 * straight to mmap. That's safe because boot is single-threaded with no
 * concurrent writer — and it means this file is a no-op drop-in until you wire
 * the control block through your workers (see control.ts / INTEGRATION.md).
 *
 * Single-writer is an invariant (each store's domain has one writer worker);
 * this guards the reader against tearing, not writer-vs-writer.
 */
export class Seqlock {
	// Vestigial on-disk pad, kept so the file layout is byte-identical to the
	// old [version:u32][payload] format — no migration for existing stores. The
	// live version now lives in the control block, not here.
	private static readonly VERSION_BYTES = 4;

	private readonly mapping: Mmap;
	private readonly payload: Uint8Array; // capacity-sized mmap view, past the pad
	public readonly capacity: number;
	private readonly id: string; // stable, identical across isolates (the file path)

	// Control-block binding, resolved lazily on first read/write.
	private control: SharedControl | null = null;
	private versionWord = -1;
	// The single writer's private, monotonic version counter — kept in-isolate
	// so parity can't be corrupted by a crashed process's leftover state.
	private writerVersion = 0;
	// One reusable snapshot buffer, so the read hot path never allocates.
	private scratch: Uint8Array;

	private constructor(mapping: Mmap, capacity: number, id: string) {
		this.mapping = mapping;
		this.capacity = capacity;
		this.id = id;
		const bytes = mapping.bytes();
		this.payload = bytes.subarray(Seqlock.VERSION_BYTES, Seqlock.VERSION_BYTES + capacity);
		this.scratch = new Uint8Array(capacity);
	}

	/** Total on-disk file size backing a seqlock with `capacity` payload bytes. */
	static fileSize(capacity: number): number {
		return Seqlock.VERSION_BYTES + capacity;
	}

	/**
	 * Open a seqlock file holding `capacity` payload bytes. `id` is the key under
	 * which the version word is claimed in the control block — pass a stable
	 * string that is identical in every isolate (the file path is ideal).
	 */
	static open(path: string, capacity: number, id: string): Seqlock {
		const mapping = Mmap.openSync(path, { write: true, ensureFileSize: Seqlock.fileSize(capacity) });
		return new Seqlock(mapping, capacity, id);
	}

	// Resolve the control block once it's available. Returns null during the
	// single-threaded boot window (safe to read/write mmap directly then).
	private bind(): SharedControl | null {
		if (this.control) return this.control;
		const control = getStorageControl();
		if (!control) return null;
		this.versionWord = control.claim(this.id, 1);
		this.control = control;
		const cur = control.loadI32(this.versionWord);
		this.writerVersion = cur & 1 ? cur + 1 : cur; // normalise to even
		return control;
	}

	/**
	 * Read a consistent snapshot of the payload. `decode` is handed a stable
	 * buffer and must not retain it. Retries until it catches an even, unchanged
	 * version (no concurrent write).
	 */
	read<T>(decode: (payload: Uint8Array) => T): T {
		const control = this.bind();
		if (!control) return decode(this.payload); // boot: no writer racing us

		const w = this.versionWord;
		const snapshot = this.scratch;
		for (;;) {
			const before = control.loadI32(w); // seqcst
			if ((before & 1) !== 0) continue; // writer mid-write
			snapshot.set(this.payload);
			const after = control.loadI32(w); // seqcst — fences the copy above
			if (before === after) return decode(snapshot);
		}
	}

	/**
	 * Publish `payload` (must be exactly `capacity` bytes). Bumps the version
	 * odd, copies, then bumps it even — so any reader mid-copy retries.
	 */
	write(payload: Uint8Array): void {
		if (payload.length !== this.capacity) {
			throw new Error(`Seqlock.write: expected ${this.capacity} bytes, got ${payload.length}`);
		}
		const control = this.bind();
		if (!control) {
			this.payload.set(payload); // boot: publish straight to mmap
			return;
		}
		const w = this.versionWord;
		const odd = this.writerVersion + 1;
		control.storeI32(w, odd); // seqcst: mark mid-write (odd)
		this.payload.set(payload); // fenced between the two stores
		control.storeI32(w, odd + 1); // seqcst: publish (even)
		this.writerVersion = odd + 1;
	}

	flush(): void {
		this.mapping.flush();
	}

	close(): void {
		this.mapping.close();
	}
}
