import { type Codec, type FixedCodec } from "@nomadshiba/codec";
import { Mmap } from "@nomadshiba/mmap";
import { join } from "@std/path";
import { Store } from "~/libs/storage/Store.ts";

const VERSION_BYTES = Uint8Array.BYTES_PER_ELEMENT; // 1 — atomic version prefix per slot
const BUSY = 255; // version 255 = a writer is mid-write; clean generations are 0..254

export type SharedArrayStoreOptions<T extends FixedCodec> = {
	path: string;
	writable: boolean;
	minChunkSize: number;
	item: T;
};

type Chunk = { mapping: Mmap; bytes: Uint8Array };

export class SharedArrayStore<T extends FixedCodec> extends Store implements Disposable {
	public readonly path: string;
	public readonly writable: boolean;
	public readonly item: T;
	public readonly slotStride: number; // version(1) + payload
	public readonly chunkSize: number;
	public readonly slotsPerChunk: number;

	private cursor: BigUint64Array;
	private cursorMmap: Mmap;
	private chunks = new Map<number, Chunk>();
	private readonly scratch: Uint8Array;

	private constructor(options: SharedArrayStoreOptions<T>) {
		super();
		this.path = options.path;
		this.writable = options.writable;
		this.item = options.item;
		this.slotStride = VERSION_BYTES + options.item.stride.size;
		this.chunkSize = Math.ceil(options.minChunkSize / this.slotStride) * this.slotStride;
		this.slotsPerChunk = this.chunkSize / this.slotStride;
		this.scratch = new Uint8Array(options.item.stride.size);

		if (this.writable) Deno.mkdirSync(this.path, { recursive: true });
		this.cursorMmap = this.openFile(join(this.path, "CURSOR"), BigUint64Array.BYTES_PER_ELEMENT);
		this.cursor = new BigUint64Array(this.cursorMmap.buffer(), 0, 1);
	}

	public static open<T extends FixedCodec>(options: SharedArrayStoreOptions<T>): SharedArrayStore<T> {
		if (!Number.isInteger(options.item.stride.size) || options.item.stride.size <= 0) {
			throw new RangeError(`item.stride.size must be a positive integer, got ${options.item.stride.size}`);
		}
		Deno.mkdirSync(options.path, { recursive: true });
		return new SharedArrayStore(options);
	}

	private openFile(file: string, bytes: number): Mmap {
		return Mmap.openSync(file, { write: this.writable, ensureFileSize: bytes });
	}

	private chunk(chunkIndex: number): Chunk {
		let chunk = this.chunks.get(chunkIndex);
		if (!chunk) {
			const mapping = this.openFile(join(this.path, String(chunkIndex)), this.chunkSize);
			chunk = { mapping, bytes: mapping.bytes() };
			this.chunks.set(chunkIndex, chunk);
		}
		return chunk;
	}

	public override size(): number {
		return Number(Atomics.load(this.cursor, 0));
	}

	public override truncate(size: number): number {
		if (!this.writable) throw new Error("SharedArrayStore is read-only");
		if (size > this.size()) {
			throw new RangeError(`truncate size=${size} is after the cursor (size=${this.size()}); truncate only moves backwards`);
		}
		return Number(Atomics.add(this.cursor, 0, BigInt(size)));
	}

	public override reveal(size: number): number {
		if (!this.writable) throw new Error("SharedArrayStore is read-only");
		if (size < this.size()) {
			throw new RangeError(`reveal size=${size} is behind the cursor (size=${this.size()}); reveal only moves forward`);
		}
		return Number(Atomics.add(this.cursor, 0, BigInt(size)));
	}

	public set(index: number, value: Codec.InferInput<T>): void {
		if (!this.writable) throw new Error("SharedArrayStore is read-only");
		const local = index % this.slotsPerChunk;
		const chunk = this.chunk((index - local) / this.slotsPerChunk);
		const versionOffset = local * this.slotStride;
		const payloadOffset = versionOffset + VERSION_BYTES;
		const generation = Atomics.load(chunk.bytes, versionOffset);
		Atomics.store(chunk.bytes, versionOffset, BUSY);
		this.item.encodeInto(value, chunk.bytes.subarray(payloadOffset, payloadOffset + this.item.stride.size));
		Atomics.store(chunk.bytes, versionOffset, generation >= BUSY - 1 ? 0 : generation + 1);
	}

	public get(index: number): Codec.InferOutput<T> {
		if (index >= this.size()) throw new RangeError();
		const local = index % this.slotsPerChunk;
		const chunk = this.chunk((index - local) / this.slotsPerChunk);
		const versionOffset = local * this.slotStride;
		const payloadOffset = versionOffset + VERSION_BYTES;
		while (true) {
			const before = Atomics.load(chunk.bytes, versionOffset);
			if (before === BUSY) continue;
			this.scratch.set(chunk.bytes.subarray(payloadOffset, payloadOffset + this.item.stride.size));
			if (Atomics.load(chunk.bytes, versionOffset) === before) {
				const [value] = this.item.decode(this.scratch);
				return value;
			}
		}
	}

	public sync(): void {
		for (const [, chunk] of this.chunks) chunk.mapping.flush();
		this.cursorMmap.flush();
	}

	public close(): void {
		for (const [, chunk] of this.chunks) chunk.mapping.close();
		this.cursorMmap.close();
	}

	public [Symbol.dispose](): void {
		this.close();
	}
}
