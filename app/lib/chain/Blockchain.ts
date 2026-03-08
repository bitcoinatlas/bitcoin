import { U32LE } from "@nomadshiba/codec";
import { ArrayStore } from "../storage/ArrayStore.ts";
import { Bytes32 } from "../codec/primitives.ts";
import { ChunkedBlobStore } from "../storage/ChunkedBlobStore.ts";
import { FixedKVStore } from "../storage/FixedKVStore.ts";
import { Block } from "./Block.ts";
import { StoredBlock } from "./codec/stored/StoredBlock.ts";
import { StoredPointer } from "./codec/stored/StoredPointer.ts";
import { WireBlockHeader } from "./codec/wire/WireBlockHeader.ts";
import { PeerChain } from "./PeerChain.ts";
import { verifyProofOfWork, workFromHeader } from "./utils/PoW.ts";
import { GENESIS_BLOCK_HEADER } from "../../constants.ts";
import { PeerChainNode } from "./PeerChainNode.ts";

export class Blockchain {
	public readonly blockHashToHeight = new FixedKVStore("./data/hashToHeight", [Bytes32, U32LE]);
	public readonly blockHeightToHeader = new ArrayStore("./data/headers", WireBlockHeader);
	public readonly blockHeightToPointer = new ArrayStore("./data/hashToHeight", U32LE);
	public readonly orderedBlocks = new ChunkedBlobStore("./data/chain");
	public readonly unorderedBlocks = new FixedKVStore("./data/blocks", [Bytes32, StoredBlock]);
	public readonly txIdToPointer = new FixedKVStore("./data/txs", [Bytes32, StoredPointer]);

	private localChain = new PeerChain([]);

	public async init() {
		const headers = await this.blockHeightToHeader.range(0, await this.blockHeightToHeader.length());
		this.localChain.clear();
		if (headers.length > 0) {
			const pointers = await this.blockHeightToPointer.range(0, await this.blockHeightToPointer.length());
			let cumulativeWork = 0n;
			this.localChain.concat(headers.map((header, height) => {
				if (!verifyProofOfWork(header)) {
					throw new Error(); // we should probably rebuild here
				}
				const pointer = pointers[height] ?? null;
				cumulativeWork += workFromHeader(header);
				return new PeerChainNode({ header, cumulativeWork, pointer });
			}));
		} else {
			this.blockHeightToPointer.truncate(0);
			this.orderedBlocks.truncate(0);
			// TODO: we also should clear the kvs.
			// also later reindexing on reorg can be weird. (reorg isnt handled here btw)
			const [header] = WireBlockHeader.decode(GENESIS_BLOCK_HEADER);
			const pointer = null;
			const cumulativeWork = workFromHeader(header);
			this.localChain.push(new PeerChainNode({ header, cumulativeWork, pointer }));
			this.blockHeightToHeader.push(header);
		}
	}

	async pushBlockHeader(headers: WireBlockHeader[]): Promise<void> {
		const oldHeight = await this.blockHeightToHeader.length();
		await this.blockHeightToHeader.concat(headers);
		await Promise.all([
			Promise.all(headers.map((header, index) => this.blockHashToHeight.set(header.hash, oldHeight + index))),
		]);
	}

	async getBlockByHeight(height: number): Promise<Block | undefined> {
		const header = await this.blockHeightToHeader.get(height);
		if (!header) return undefined;
		return new Block(header);
	}

	async getBlockByHash(hash: Uint8Array): Promise<Block | undefined> {
		const height = await this.blockHashToHeight.get(hash);
		if (height === undefined) {
			throw new Error("Not Implemented: fetching blocks from peers");
		}
		const header = await this.blockHeightToHeader.get(height);
		if (!header) {
			return undefined;
		}
		return new Block(header);
	}
}
