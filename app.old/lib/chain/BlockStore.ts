import { exists, existsSync } from "@std/fs";
import { dirname, join } from "@std/path";
import { BlockHeightIndex } from "./BlockHeightIndex.ts";
import { StoredBlock } from "./old/StoredBlock.ts";
import { readFileExact } from "../fs.ts";
import { MAX_BLOCK_WEIGHT } from "../constants.ts";

export namespace BlockStore {
	export type Init = {
		maxFileSize: number;
		baseDirectory: string;
		blockPointerIndex: BlockHeightIndex;
	};
}

export class BlockStore {
	private readonly baseDirectory: string;
	private readonly maxFileSize: number;
	private readonly pointerIndex: BlockHeightIndex;

	private pointer: number;

	constructor(init: BlockStore.Init) {
		this.baseDirectory = init.baseDirectory;
		this.pointerIndex = init.blockPointerIndex;
		this.maxFileSize = init.maxFileSize;

		this.pointer = this.pointerIndex.tip() ?? 0;
	}

	private getFilePath(chunkId: number): string {
		const filename = `chunk${chunkId}.dat`;
		return join(this.baseDirectory, filename);
	}

	public async append(block: StoredBlock): Promise<number> {
		const blockBytes = StoredBlock.encode(block);

		const filePath = this.getFilePath(Math.floor((this.pointer + blockBytes.byteLength) / this.maxFileSize));
		await Deno.mkdir(dirname(filePath), { recursive: true });
		await Deno.writeFile(filePath, blockBytes, { append: true, create: true });

		const height = await this.pointerIndex.append([this.pointer]);

		this.pointer += blockBytes.byteLength;

		return height;
	}

	public async truncate(blockHeight: number): Promise<void> {
		const pointer = this.pointerIndex.at(blockHeight);
		if (pointer === undefined) {
			throw new Error(`No block found at height ${blockHeight}`);
		}

		this.pointer = pointer;
		const chunkId = Math.floor(this.pointer / this.maxFileSize);
		const chunkOffset = this.pointer % this.maxFileSize;
		const chunkFilePath = this.getFilePath(chunkId);

		if (!await exists(chunkFilePath)) {
			throw new Error(`Block file ${chunkFilePath} does not exist`);
		}

		await Deno.truncate(chunkFilePath, chunkOffset);

		// Truncate any subsequent files
		let nextChunkId = chunkId + 1;
		while (true) {
			const nextChunkFilePath = this.getFilePath(nextChunkId);
			if (await exists(nextChunkFilePath)) {
				await Deno.remove(nextChunkFilePath);
				nextChunkId++;
			} else {
				break;
			}
		}

		this.pointerIndex.truncate(blockHeight);
	}

	public async read(pointer: number, length: number): Promise<Uint8Array> {
		const fileId = Math.floor(pointer / this.maxFileSize);
		const fileOffset = pointer % this.maxFileSize;

		const path = this.getFilePath(fileId);
		if (!await exists(path)) {
			throw new Error(`Block file ${path} does not exist`);
		}

		const file = await Deno.open(path, { read: true });
		const fileStat = await file.stat();
		try {
			await file.seek(fileOffset, Deno.SeekMode.Start);
			const buffer = new Uint8Array(Math.min(length, fileStat.size - fileOffset));
			await readFileExact(file, buffer);
			return buffer;
		} finally {
			file.close();
		}
	}

	public async update(pointer: number, data: Uint8Array): Promise<void> {
		const fileId = Math.floor(pointer / this.maxFileSize);
		const fileOffset = pointer % this.maxFileSize;

		const path = this.getFilePath(fileId);
		if (!existsSync(path)) {
			throw new Error(`Block file ${path} does not exist`);
		}

		const file = await Deno.open(path, { read: true, write: true });
		try {
			await file.seek(fileOffset, Deno.SeekMode.Start);
			await file.write(data);
		} finally {
			file.close();
		}
	}

	public async block(height: number): Promise<StoredBlock> {
		const pointer = this.pointerIndex.at(height);
		if (pointer === undefined) {
			throw new Error(`No block found at height ${height}`);
		}

		const next4Mb = await this.read(pointer, MAX_BLOCK_WEIGHT);
		const [block] = StoredBlock.decode(next4Mb);
		return block;
	}

	public height(): number {
		return this.pointerIndex.height();
	}
}
