/**
 * One process-wide shared-memory control block, handed to every worker as a
 * single {@linkcode SharedArrayBuffer}. A store registers a small run of i32
 * words under a stable string id (its file path) and gets back a word index;
 * every isolate that claims the same id lands on the SAME words.
 *
 * Why this exists: `Atomics` only emit a real cross-isolate memory fence
 * (`dmb ish` / `mfence`) when the buffer is a `SharedArrayBuffer`. On the
 * non-shared `ArrayBuffer` that wraps an mmap pointer, `Atomics` are atomic on
 * the word but carry NO ordering across isolates — fine on x86-TSO, a latent
 * tearing bug on ARM. Our workers are same-process isolates, so a SAB reaches
 * all of them. The mmap stays the persistence + bulk-data layer; only the tiny
 * seqlock *version word* moves here, which is all the fence needs to attach to.
 *
 * Layout (i32 words):
 *   [0] alloc lock (CAS 0<->1, startup only)
 *   [1] bump pointer (next free word index)
 *   [2 .. 2+TABLE_WORDS)  open-addressing table: TABLE_CAP × (idHash, wordIndex)
 *   [WORDS_BASE ..]       the claimed words
 */
export class SharedControl {
	private static readonly LOCK = 0;
	private static readonly BUMP = 1;
	private static readonly HEADER_WORDS = 2;
	private static readonly TABLE_CAP = 256;
	private static readonly TABLE_WORDS = SharedControl.TABLE_CAP * 2;
	private static readonly WORDS_BASE = SharedControl.HEADER_WORDS + SharedControl.TABLE_WORDS;

	/** The buffer to forward to workers (`postMessage(..., )` keeps it shared). */
	readonly sab: SharedArrayBuffer;
	private readonly i32: Int32Array;

	constructor(sab: SharedArrayBuffer) {
		this.sab = sab;
		this.i32 = new Int32Array(sab);
	}

	/** Create the process-root control block. Call once, in the root isolate. */
	static create(bytes = 64 * 1024): SharedControl {
		if (bytes % 4 !== 0) throw new Error("SharedControl size must be a multiple of 4");
		const c = new SharedControl(new SharedArrayBuffer(bytes));
		// A fresh SAB is zero-filled → lock=0, every table hash=0 (empty). We
		// only need to point the bump past the table.
		Atomics.store(c.i32, SharedControl.BUMP, SharedControl.WORDS_BASE);
		return c;
	}

	/**
	 * Reserve `words` i32 words for `id`, or return the run a prior claim of the
	 * same id already reserved — idempotent across isolates, so the writer and
	 * every reader independently `claim(path, 1)` and all land on one word.
	 *
	 * Startup-only, so a CAS spinlock over the tiny table is plenty; this is
	 * never on a read path.
	 *
	 * NOTE: ids are matched by 32-bit hash. For a handful of distinct store
	 * paths a collision is astronomically unlikely; if you ever want it
	 * impossible, store the id bytes in the table and compare. Keep ids stable
	 * and identical across isolates (the file path is ideal).
	 */
	claim(id: string, words: number): number {
		const h = SharedControl.hash(id);
		while (Atomics.compareExchange(this.i32, SharedControl.LOCK, 0, 1) !== 0) {
			// spin — claims are rare and brief
		}
		try {
			const cap = SharedControl.TABLE_CAP;
			let slot = (h >>> 0) % cap;
			for (let probe = 0; probe < cap; probe++) {
				const entry = SharedControl.HEADER_WORDS + slot * 2;
				const stored = Atomics.load(this.i32, entry);
				if (stored === 0) {
					// empty slot → allocate a fresh run
					const base = Atomics.load(this.i32, SharedControl.BUMP);
					const next = base + words;
					if (next > this.i32.length) throw new Error("SharedControl out of space — raise create() size");
					Atomics.store(this.i32, SharedControl.BUMP, next);
					Atomics.store(this.i32, entry + 1, base);
					Atomics.store(this.i32, entry, h); // publish the hash last
					return base;
				}
				if (stored === h) return Atomics.load(this.i32, entry + 1); // already claimed
				slot = (slot + 1) % cap; // linear probe
			}
			throw new Error("SharedControl table full — raise TABLE_CAP");
		} finally {
			Atomics.store(this.i32, SharedControl.LOCK, 0);
		}
	}

	/** Sequentially-consistent load of a claimed word (acquire, and then some). */
	loadI32(word: number): number {
		return Atomics.load(this.i32, word);
	}

	/** Sequentially-consistent store to a claimed word (release, and then some). */
	storeI32(word: number, value: number): void {
		Atomics.store(this.i32, word, value);
	}

	// 32-bit FNV-1a, forced non-zero (0 is the empty-slot sentinel).
	private static hash(id: string): number {
		let h = 0x811c9dc5;
		for (let i = 0; i < id.length; i++) {
			h ^= id.charCodeAt(i);
			h = Math.imul(h, 0x01000193);
		}
		h >>>= 0;
		return h === 0 ? 1 : h;
	}
}
