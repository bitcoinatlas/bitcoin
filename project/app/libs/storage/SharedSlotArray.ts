import { Mmap } from "@nomadshiba/mmap";
import { join } from "@std/path";

const BYTES_PER_SLOT = BigUint64Array.BYTES_PER_ELEMENT; // 8

export type SharedSlotArrayOptions = {
	path: string;
	writable: boolean;
	minChunkSize: number;
};

type Chunk = { mapping: Mmap; slots: BigUint64Array };

export class SharedSlotArray implements Disposable {
	public readonly path: string;
	public readonly writable: boolean;
	public readonly chunkSize: number;
	public readonly slotsPerChunk: number;

	public static BYTES_PER_SLOT = BYTES_PER_SLOT;

	private cursor: BigUint64Array;
	private cursorMmap: Mmap;
	private chunks = new Map<number, Chunk>();

	private constructor(options: SharedSlotArrayOptions) {
		this.path = options.path;
		this.writable = options.writable;
		this.chunkSize = Math.ceil(options.minChunkSize / BYTES_PER_SLOT) * BYTES_PER_SLOT;
		this.slotsPerChunk = this.chunkSize / BYTES_PER_SLOT;

		if (this.writable) Deno.mkdirSync(this.path, { recursive: true });
		this.cursorMmap = this.open(join(this.path, "CURSOR"), BYTES_PER_SLOT);
		this.cursor = new BigUint64Array(this.cursorMmap.buffer(), 0, 1);
	}

	public static open(options: SharedSlotArrayOptions): SharedSlotArray {
		Deno.mkdirSync(options.path, { recursive: true });
		return new SharedSlotArray(options);
	}

	private open(file: string, bytes: number): Mmap {
		// Always ensure the backing file is at least `bytes` long, even read-only:
		// on a fresh data dir a read-only opener (a non-committer worker, or the
		// main isolate) can reach a store before its committer has created the
		// file, and mapping a zero-length file throws. An all-zero region is a
		// valid empty state (cursor 0 / all buckets empty), so this only ever
		// creates the same empty file the committer would have. The mapping itself
		// stays read-only when this worker isn't the committer.
		return Mmap.openSync(file, { write: this.writable, ensureFileSize: bytes });
	}

	private chunk(chunkIndex: number): BigUint64Array {
		let chunk = this.chunks.get(chunkIndex);
		if (!chunk) {
			const mapping = this.open(join(this.path, String(chunkIndex)), this.chunkSize);
			const slots = new BigUint64Array(mapping.buffer(), 0, this.slotsPerChunk);
			chunk = { mapping, slots };
			this.chunks.set(chunkIndex, chunk);
		}
		return chunk.slots;
	}

	public size(): number {
		return Number(Atomics.load(this.cursor, 0));
	}

	public resize(count: number): number {
		if (!this.writable) throw new Error("SharedSlotArray is read-only");
		return Number(Atomics.add(this.cursor, 0, BigInt(count)));
	}

	public set(index: number, value: bigint): void {
		if (!this.writable) throw new Error("SharedSlotArray is read-only");
		const local = index % this.slotsPerChunk;
		Atomics.store(this.chunk((index - local) / this.slotsPerChunk), local, value);
	}

	public get(index: number): bigint {
		if (index >= this.size()) throw new RangeError();
		const local = index % this.slotsPerChunk;
		return Atomics.load(this.chunk((index - local) / this.slotsPerChunk), local);
	}

	public sync(): void {
		for (const [, chunk] of this.chunks) chunk.mapping.flush();
		this.cursorMmap.flush();
	}

	public close() {
		for (const [, chunk] of this.chunks) chunk.mapping.close();
		this.cursorMmap.close();
	}

	public [Symbol.dispose](): void {
		this.close();
	}
}
