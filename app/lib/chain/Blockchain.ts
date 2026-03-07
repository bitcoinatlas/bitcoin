import { WireBlock } from "~/lib/chain/codec/wire/WireBlock.ts";
import { CachedArrayStore } from "~/lib/storage/CachedArrayStore.ts";
import { Bytes32 } from "../codec/primitives.ts";
import { FixedKVStore } from "../storage/FixedKVStore.ts";
import { Block } from "./Block.ts";
import { StoredBlock } from "./codec/stored/StoredBlock.ts";

export class Blockchain {
	#blockHeaderStore = new CachedArrayStore("./data/headers", WireBlock.shape.header);
	#unorderedBlocks = new FixedKVStore("./data/blocks", { codecs: [Bytes32, StoredBlock] });

	async pushBlockHeaders(headers: WireBlock["header"][]): Promise<void> {
		await this.#blockHeaderStore.pushMany(headers);
	}

	async getBlockByHeight(height: number): Promise<Block | undefined> {
		const header = await this.#blockHeaderStore.get(height);
		if (!header) return undefined;
		return new Block(header);
	}
}
