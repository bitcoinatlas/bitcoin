import { sha256 } from "@noble/hashes/sha2";
import { equals } from "@std/bytes";
import { existsSync } from "@std/fs";
import { dirname, join } from "@std/path";
import { Bytes } from "~/lib/codec/bytes.ts";
import { BlockHeader } from "~/lib/satoshi/primitives/BlockHeader.ts";
import { Chain } from "./Chain.ts";
import { ChainNode } from "./ChainNode.ts";
import { verifyProofOfWork, workFromHeader } from "~/lib/chain/utils.ts";
import { readFileExact } from "../fs.ts";

const itemSchema = Bytes.fixed(BlockHeader.stride);

export class ChainStore {
	public readonly path: string;
	constructor(path: string) {
		this.path = path;
	}

	public async append(headers: Iterable<ChainNode>): Promise<void> {
		await Deno.mkdir(dirname(this.path), { recursive: true });
		console.log(`Saving headers to ${this.path}`);
		const file = await Deno.open(this.path, { append: true, create: true });
		const writer = file.writable.getWriter();
		for (const { header } of headers) {
			await writer.write(
				Bytes.encode(itemSchema, header),
			);
		}
		file.close();
		console.log(`Finished saving headers to ${this.path}`);
	}

	public async truncate(height: number): Promise<void> {
		const size = (height + 1) * itemSchema.stride;
		const path = join(this.path);
		const file = await Deno.open(path, { read: true, write: true });
		await file.truncate(size);
		file.close();
	}

	public async load(chain: Chain): Promise<void> {
		const path = this.path;
		const size = existsSync(path) ? Deno.statSync(path).size : 0;
		if (size % itemSchema.stride !== 0) {
			throw new Error("Invalid headers.dat file, size is not a multiple of header size");
		}

		chain.clear();

		if (size > 0) {
			await Deno.mkdir(dirname(path), { recursive: true });
			const file = await Deno.open(path, { read: true });
			const headerCount = size / itemSchema.stride;
			console.log(`Loading ${headerCount} headers from ${path}`);

			for (let i = 0; i < headerCount; i++) {
				const itemBytes = new Uint8Array(itemSchema.stride);
				await readFileExact(file, itemBytes);
				const [header] = Bytes.decode(itemSchema, itemBytes);
				const prevHash = header.subarray(
					BlockHeader.shape.version.stride,
					BlockHeader.shape.version.stride + BlockHeader.shape.prevHash.stride,
				);
				if (!equals(prevHash, chain.tip().hash)) {
					throw new Error(`Headers do not form a chain at height ${i}`);
				}
				const hash = sha256(sha256(header));
				if (!verifyProofOfWork(header, hash)) {
					throw new Error(`Invalid proof of work at height ${i}`);
				}
				const cumulativeWork = chain.tip().cumulativeWork + workFromHeader(header);
				chain.append({ hash, header, cumulativeWork });
			}
			file.close();
			console.log(
				`Loaded ${headerCount} headers. Height=${chain.height()} Work=${chain.tip().cumulativeWork}`,
			);
		}
	}
}
