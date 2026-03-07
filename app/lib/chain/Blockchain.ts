import { sha256 } from "@noble/hashes/sha2";
import { StringCodec, u64 } from "@nomadshiba/codec";
import { blockCodec, BlockHeaderData } from "../codec/Block.ts";
import { TxData } from "../codec/Tx.ts";
import { CachedArrayStore } from "../storage/CachedArrayStore.ts";
import { FixedKVStore } from "../storage/FixedKVStore.ts";

export class Blockchain {
	#blockHeaderStore = new CachedArrayStore("./data/headers", blockCodec.shape.header);
	#heightMetaStore = new FixedKVStore("./data/height.meta", { keyCodec: new StringCodec(), valueCodec: u64 });

	public async getVerificationHeight() {
		return await this.#heightMetaStore.get("verificationHeight");
	}

	public async setVerificationHeight(height: bigint) {
		return await this.#heightMetaStore.set("verificationHeight", height);
	}

	public async getOrderedDownloadHeight() {
		return await this.#heightMetaStore.get("orderedDownloadHeight");
	}

	public async setOrderedDownloadHeight(height: bigint) {
		return await this.#heightMetaStore.set("orderedDownloadHeight", height);
	}

	async pushBlockHeader(header: BlockHeaderData) {
		await this.#blockHeaderStore.push(header);
	}

	async getBlockByHeight(height: number): Promise<Block | undefined> {
		const header = await this.#blockHeaderStore.get(height);
		if (!header) return undefined;
		return new Block(header);
	}
}

export class Block {
	public readonly header: BlockHeaderData;
	public readonly headerHash: Uint8Array;
	public async txs(): Promise<TxData[]> {
		return [];
	}

	constructor(header: BlockHeaderData) {
		this.header = header;
		// There are not many headers, so probably this wont be too slow, but in case it becomes an issue, do the same thing you did with the transactions.
		this.headerHash = sha256(sha256(blockCodec.shape.header.encode(header)));
	}
}
