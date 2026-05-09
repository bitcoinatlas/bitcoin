import { U32LE } from "@nomadshiba/codec";
import { type ArrayStore, createArrayStore } from "../storage/ArrayStore.ts";
import { createKVStore, type KVStore } from "../storage/KVStore.ts";
import { type BlobStore, createBlobStore } from "../storage/BlobStore.ts";
import { Bytes32, U48LE } from "../codec/primitives.ts";
import { Block } from "./Block.ts";
import { StoredBlock } from "./codec/stored/StoredBlock.ts";
import { StoredPointer } from "./codec/stored/StoredPointer.ts";
import { WireBlockHeader } from "./codec/wire/WireBlockHeader.ts";
import { PeerChain } from "./PeerChain.ts";
import { verifyProofOfWork, workFromHeader } from "./utils/PoW.ts";
import { GENESIS_BLOCK_HEADER } from "../../constants.ts";
import { PeerChainNode } from "./PeerChainNode.ts";
import { atomic } from "../storage/Store.ts";
import { join } from "@std/path";
import { BASE_DATA_DIR } from "../../constants.ts";

export class Blockchain {
	public blockHashToHeight!: KVStore<Uint8Array, number>;
	public blockHeightToHeader!: ArrayStore<WireBlockHeader>;
	public blockHeightToPointer!: ArrayStore<StoredPointer>;
	public orderedBlocks!: BlobStore;
	public unorderedBlocks!: KVStore<Uint8Array, StoredBlock>;
	public txIdToPointer!: KVStore<Uint8Array, StoredPointer>;

	private localChain = new PeerChain([]);

	public async init() {
		this.blockHashToHeight = await createKVStore({
			name: "hashToHeight",
			path: join(BASE_DATA_DIR, "hashToHeight"),
			keyCodec: Bytes32,
			valueCodec: U32LE,
		});
		this.blockHeightToHeader = await createArrayStore({
			name: "headers",
			path: join(BASE_DATA_DIR, "headers"),
			codec: WireBlockHeader,
			countCodec: U48LE,
		});
		this.blockHeightToPointer = await createArrayStore({
			name: "pointers",
			path: join(BASE_DATA_DIR, "pointers"),
			codec: StoredPointer,
			countCodec: U48LE,
		});
		this.orderedBlocks = await createBlobStore({
			name: "chain",
			path: join(BASE_DATA_DIR, "chain"),
		});
		this.unorderedBlocks = await createKVStore({
			name: "blocks",
			path: join(BASE_DATA_DIR, "blocks"),
			keyCodec: Bytes32,
			valueCodec: StoredBlock,
		});
		this.txIdToPointer = await createKVStore({
			name: "txs",
			path: join(BASE_DATA_DIR, "txs"),
			keyCodec: Bytes32,
			valueCodec: StoredPointer,
		});

		const headerLength = this.blockHeightToHeader.length();
		const headers = await this.blockHeightToHeader.slice(0, headerLength);
		this.localChain.clear();
		if (headers.length > 0) {
			const pointers = await this.blockHeightToPointer.slice(0, this.blockHeightToPointer.length());
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
			await this.blockHeightToPointer.truncate(0);
			await this.orderedBlocks.truncate(0);
			// TODO: clear kvs on full reindex; reorg not handled here
			const [header] = WireBlockHeader.decode(GENESIS_BLOCK_HEADER);
			const pointer = null;
			const cumulativeWork = workFromHeader(header);
			this.localChain.push(new PeerChainNode({ header, cumulativeWork, pointer }));
			const tx = this.blockHeightToHeader.transaction();
			tx.append(header);
			tx.apply();
			await atomic([this.blockHeightToHeader]);
		}
	}

	async pushBlockHeader(headers: WireBlockHeader[]): Promise<void> {
		const oldHeight = this.blockHeightToHeader.length();

		const headerTx = this.blockHeightToHeader.transaction();
		for (const header of headers) headerTx.append(header);
		headerTx.apply();

		const kvTx = this.blockHashToHeight.transaction();
		for (let i = 0; i < headers.length; i++) {
			kvTx.set(headers[i]!.hash, oldHeight + i);
		}
		kvTx.apply();

		await atomic([this.blockHeightToHeader, this.blockHashToHeight]);
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
		if (!header) return undefined;
		return new Block(header);
	}
}
