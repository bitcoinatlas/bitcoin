import { existsSync } from "@std/fs";
import { dirname, join } from "@std/path";
import { readFileExactSync } from "../fs.ts";

export class HeightStore<T> {
	public readonly codec: BoundCodec<T>;
	public readonly path: string;
	private readonly memory: T[];

	constructor(codec: BoundCodec<T>, path: string) {
		this.codec = codec;
		this.path = path;
		this.memory = [];
		this.reload();
	}

	public reload(): void {
		const path = this.path;
		const size = existsSync(path) ? Deno.statSync(path).size : 0;
		if (size % this.codec.stride !== 0) {
			throw new Error("Invalid index file, size is not a multiple of item size");
		}

		this.memory.length = 0;

		if (size > 0) {
			Deno.mkdirSync(dirname(path), { recursive: true });
			const file = Deno.openSync(path, { read: true });
			const itemCount = size / this.codec.stride;
			console.log(`Loading ${itemCount} items from ${path}`);

			for (let i = 0; i < itemCount; i++) {
				const itemBytes = new Uint8Array(this.codec.stride);
				readFileExactSync(file, itemBytes);
				const [item] = this.codec.decode(itemBytes);
				this.memory.push(item);
			}
			file.close();
		}
	}

	public async append(items: Iterable<T>): Promise<number> {
		await Deno.mkdir(dirname(this.path), { recursive: true });
		const file = await Deno.open(this.path, { append: true, create: true });
		const writer = file.writable.getWriter();
		for (const item of items) {
			await writer.write(this.codec.encode(item));
		}
		file.close();
		this.memory.push(...items);
		return this.memory.length - 1;
	}

	public async truncate(height: number): Promise<void> {
		const size = (height + 1) * this.codec.stride;
		const path = join(this.path);
		const file = await Deno.open(path, { read: true, write: true });
		await file.truncate(size);
		file.close();
		this.memory.length = height + 1;
	}

	public async clear(): Promise<void> {
		const path = join(this.path);
		if (existsSync(path)) {
			await Deno.remove(path);
		}
		this.memory.length = 0;
	}

	[Symbol.iterator](): ArrayIterator<Readonly<T>> {
		return this.memory.values();
	}

	public entries(): ArrayIterator<[number, Readonly<T>]> {
		return this.memory.entries();
	}

	public values(): ArrayIterator<Readonly<T>> {
		return this.memory.values();
	}

	public height(): number {
		return this.memory.length - 1;
	}

	public tip(): T | undefined {
		return this.memory.at(-1);
	}

	public at(height: number): T | undefined {
		return this.memory.at(height);
	}

	public length(): number {
		return this.memory.length;
	}
}
