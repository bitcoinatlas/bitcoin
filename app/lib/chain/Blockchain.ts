import { U32, U32LE } from "@nomadshiba/codec";
import { CachedArrayStore } from "~/lib/storage/CachedArrayStore.ts";
import { Bytes32 } from "../codec/primitives.ts";
import { ChunkedBlobStore } from "../storage/ChunkedBlobStore.ts";
import { FixedKVStore } from "../storage/FixedKVStore.ts";
import { Block } from "./Block.ts";
import { StoredBlock } from "./codec/stored/StoredBlock.ts";
import { WireBlockHeader } from "./codec/wire/WireBlockHeader.ts";
import { StoredPointer } from "./codec/stored/StoredPointer.ts";

export class Blockchain {
	public readonly blockHeaders = new CachedArrayStore("./data/headers", WireBlockHeader);
	public readonly blockHashToHeight = new FixedKVStore("./data/hashToHeight", [Bytes32, U32LE]);

	public readonly orderedBlock = new ChunkedBlobStore("./data/chain");
	public readonly blockHeightToPointer = new FixedKVStore("./data/hashToHeight", [U32, U32LE]);

	public readonly unorderedBlock = new FixedKVStore("./data/blocks", [Bytes32, StoredBlock]);

	public readonly txIdToPointer = new FixedKVStore("./data/txs", [Bytes32, StoredPointer]);

	async pushBlockHeader(headers: WireBlockHeader[]): Promise<void> {
		const oldHeight = await this.blockHeaders.length();
		await this.blockHeaders.pushMany(headers);
		await Promise.all([
			Promise.all(headers.map((header, index) => this.blockHashToHeight.set(header.hash, oldHeight + index))),
		]);
	}

	async getBlockByHeight(height: number): Promise<Block | undefined> {
		const header = await this.blockHeaders.get(height);
		if (!header) return undefined;
		return new Block(header);
	}

	async getBlockByHash(hash: Uint8Array): Promise<Block | undefined> {
		const height = await this.blockHashToHeight.get(hash);
		if (height === undefined) {
			throw new Error("Not Implemented: fetching blocks from peers");
		}
		const header = await this.blockHeaders.get(height);
		if (!header) {
			return undefined;
		}
		return new Block(header);
	}
}
