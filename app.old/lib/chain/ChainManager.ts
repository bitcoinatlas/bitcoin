import { bytesToNumberLE } from "@noble/curves/abstract/utils";
import { sha256 } from "@noble/hashes/sha2";
import { equals } from "@std/bytes";
import { join } from "@std/path";
import { verifyProofOfWork, workFromHeader } from "~/lib/chain/utils.ts";
import { CompactSize } from "~/lib/CompactSize.ts";
import { GENESIS_BLOCK_HASH, GENESIS_BLOCK_HEADER } from "~/lib/constants.ts";
import { humanize } from "~/lib/logging/human.ts";
import { GetHeadersMessage } from "~/lib/satoshi/p2p/messages/GetHeaders.ts";
import { HeadersMessage } from "~/lib/satoshi/p2p/messages/Headers.ts";
import { Peer } from "~/lib/satoshi/p2p/Peer.ts";
import { PeerManager } from "~/lib/satoshi/p2p/PeerManager.ts";
import { BlockHeader } from "~/lib/satoshi/primitives/BlockHeader.ts";
import { BlockDownloader } from "./BlockDownloader.ts";
import { BlockHeightIndex } from "./BlockHeightIndex.ts";
import { BlockStore } from "./BlockStore.ts";
import { Chain } from "./Chain.ts";
import { ChainStore } from "./ChainStore.ts";
import { StoredBlock } from "./primitives/StoredBlock.ts";

export class ChainManager {
	private readonly baseDirectory: string;
	private readonly workerCount: number;

	private localChain: Chain;
	private readonly chainStore: ChainStore;
	private readonly blockStore: BlockStore;
	private readonly blockDownloader: BlockDownloader;
	private readonly hashToHeight: Map<bigint, number>;
	private readonly prevHashToHeight: Map<bigint, number>;
	private readonly blacklist = new Set<bigint>();

	constructor(baseDirectory: string, peerManager: PeerManager, workerCount = navigator.hardwareConcurrency || 4) {
		console.log(`Using ${workerCount} workers`);
		this.workerCount = workerCount;
		this.baseDirectory = baseDirectory;

		this.localChain = new Chain([]);
		this.chainStore = new ChainStore(join(this.baseDirectory, "headers.dat"));
		this.blockStore = new BlockStore({
			baseDirectory: join(this.baseDirectory, "blocks"),
			maxFileSize: 128 * 1024 * 1024, // 128 MB
			blockPointerIndex: new BlockHeightIndex(join(this.baseDirectory, "block_pointer_idx.dat")),
		});
		this.blockDownloader = new BlockDownloader(peerManager);
		this.hashToHeight = new Map();
		this.prevHashToHeight = new Map();
	}

	public async init(): Promise<void> {
		await this.chainStore.load(this.localChain);
		if (this.localChain.length() === 0) {
			const genesisWork = workFromHeader(GENESIS_BLOCK_HASH);
			this.localChain.append({
				header: GENESIS_BLOCK_HEADER,
				hash: GENESIS_BLOCK_HASH,
				cumulativeWork: genesisWork,
			});
			await this.chainStore.append(this.localChain.values());
			console.log(`Initialized new chain with genesis block, work=${genesisWork}`);
		}

		this.reindexChain();
	}

	private reindexChain(): void {
		this.prevHashToHeight.clear();
		this.hashToHeight.clear();
		for (const [height, { header, hash }] of this.localChain.entries()) {
			const prevHash = header.subarray(
				BlockHeader.shape.version.stride,
				BlockHeader.shape.version.stride + BlockHeader.shape.prevHash.stride,
			);
			this.prevHashToHeight.set(bytesToNumberLE(prevHash), height);
			this.hashToHeight.set(bytesToNumberLE(hash), height);
		}
	}

	private peersAtTip: WeakSet<Peer> = new WeakSet();
	async syncHeadersFromPeers(peerManager: PeerManager): Promise<void> {
		const CHUNK_SIZE = 210_000; // Max headers to fetch per call to prevent infinite header attacks

		const peers = peerManager.peers();

		for (const peer of peers) {
			// Skip if peer is no longer connected
			if (!peer.connected) {
				continue;
			}

			if (this.peersAtTip.has(peer)) {
				continue;
			}

			try {
				const targetHeight = this.localChain.height() + CHUNK_SIZE;
				const peerChain = await this.syncHeadersFromPeer(peer, targetHeight);
				const reachedTip = peerChain.height() < targetHeight;
				if (reachedTip) {
					this.peersAtTip.add(peer);
					peer.log(`Reached tip of peer's chain at height ${peerChain.height()}`);
				}
			} catch (e) {
				peer.logError(`Failed to sync headers:`, e);
			}
		}
	}

	private async syncHeadersFromPeer(peer: Peer, targetHeight: number): Promise<Chain> {
		const peerChain = new Chain(Array.from(this.localChain));
		let chainSplit: { commonHeight: number } | null = null;

		const locators: Uint8Array[] = [];
		let step = 1;
		let index = peerChain.height();
		while (index >= 0) {
			locators.push(peerChain.at(index)!.hash);
			if (locators.length >= 10) step <<= 1;
			index -= step;
		}
		if (!equals(locators.at(-1)!, GENESIS_BLOCK_HASH)) {
			locators.push(GENESIS_BLOCK_HASH);
		}

		while (true) {
			// Check connection before each request
			if (!peer.connected) {
				throw new Error("Peer disconnected during sync");
			}

			const headersPromise = peer.expectRaw(HeadersMessage);
			await peer.send(GetHeadersMessage, {
				version: 70015,
				locators,
				stopHash: new Uint8Array(32),
			});
			const headers = await headersPromise;
			const [count, countSize] = CompactSize.decode(headers, 0);
			if (count === 0) {
				// No more headers from peer - reached tip
				await this.commitPeerChain(peer, peerChain, chainSplit);
				return peerChain;
			}

			// Check if we've reached target height
			if (peerChain.height() >= targetHeight) {
				await this.commitPeerChain(peer, peerChain, chainSplit);
				return peerChain;
			}

			const firstPrevHash = headers.subarray(
				countSize + BlockHeader.shape.version.stride,
				countSize + BlockHeader.shape.version.stride + BlockHeader.shape.prevHash.stride,
			);

			if (!equals(firstPrevHash, peerChain.tip().hash)) {
				if (chainSplit) {
					throw new Error("Chain split twice from same peer, aborting. Peer's own chain changed?");
				}
				chainSplit = { commonHeight: this.hashToHeight.get(bytesToNumberLE(firstPrevHash)) ?? 0 };

				peerChain.truncate(chainSplit.commonHeight);
				peer.log(`Rewound to height ${chainSplit.commonHeight} to resolve a fork`);
			}

			for (let i = 0; i < count; i++) {
				const headerOffset = countSize + i * (BlockHeader.stride + 1);
				const header = headers.subarray(headerOffset, headerOffset + BlockHeader.stride);
				const height = peerChain.height() + 1;
				const prevHash = header.subarray(
					BlockHeader.shape.version.stride,
					BlockHeader.shape.version.stride + BlockHeader.shape.prevHash.stride,
				);
				if (!equals(prevHash, peerChain.tip().hash)) {
					peer.logWarn(humanize(prevHash), humanize(peerChain.tip().hash));
					peer.logWarn(`Headers do not form a chain at height ${height}`);
					// Accept partial chain if cumulative work is higher
					await this.commitPeerChain(peer, peerChain, chainSplit);
					return peerChain;
				}

				const hash = sha256(sha256(header));
				if (this.blacklist.has(bytesToNumberLE(hash))) {
					peer.logWarn(`Peer sent a banned block at height ${height}`);
					await this.commitPeerChain(peer, peerChain, chainSplit);
					return peerChain;
				}
				if (!verifyProofOfWork(header, hash)) {
					peer.logWarn(`Invalid proof of work at height ${height}`);
					await this.commitPeerChain(peer, peerChain, chainSplit);
					return peerChain;
				}
				const cumulativeWork = peerChain.tip().cumulativeWork + workFromHeader(header);
				peerChain.append({ hash, header, cumulativeWork });
			}

			locators[0] = peerChain.tip().hash;
			locators.length = 1;
			peer.log(
				`Downloaded ${peerChain.height()} headers, latest: ${
					humanize(peerChain.tip().hash)
				}, work=${peerChain.tip().cumulativeWork}`,
			);
		}
	}

	private async commitPeerChain(
		peer: Peer,
		peerChain: Chain,
		chainSplit: { commonHeight: number } | null,
	): Promise<void> {
		// commit only if cumulative work improved (true "longest" chain)
		if (peerChain.tip().cumulativeWork > this.localChain.tip().cumulativeWork) {
			peer.log(`Updating local chain: height ${this.localChain.height()} → ${peerChain.length() - 1}`);
			if (chainSplit) {
				await this.chainStore.truncate(chainSplit.commonHeight);
				await this.blockStore.truncate(chainSplit.commonHeight);
				const commonLength = chainSplit.commonHeight + 1;
				await this.chainStore.append(peerChain.values().drop(commonLength));
			} else {
				await this.chainStore.append(peerChain.values().drop(this.localChain.length()));
			}

			this.localChain = peerChain;
			this.reindexChain();
			peer.log(
				`Local chain updated. Height=${this.localChain.height()} Work=${this.localChain.tip().cumulativeWork}`,
			);
		} else {
			peer.log(
				`Kept existing tip. Height=${this.localChain.height()} Work=${this.localChain.tip().cumulativeWork}`,
			);
		}
	}

	public async downloadBlocks(batchSize: number): Promise<void> {
		const startHeight = this.blockStore.height() + 1;
		const targetHeight = Math.min(this.localChain.height() + 1, startHeight + batchSize);
		if (startHeight >= targetHeight) {
			console.log(`No new blocks to download. Current height: ${startHeight}`);
			return;
		}

		const hashes = this.localChain.values()
			.drop(startHeight)
			.take(targetHeight - startHeight)
			.map((node) => node.hash);
		const blocks = await this.blockDownloader.downloadBatch(hashes);
		for (const block of blocks) {
			const height = await this.blockStore.append(StoredBlock.fromBlock(block));
			/* // TODO: remove this later. read back and verify
			const storedBlock = await this.blockStore.block(height);
			if (!storedBlock) {
				throw new Error(`No stored block at height ${height}`);
			}
			const coinbase = block.txs[0]?.vin[0]?.scriptSig;
			if (!coinbase) {
				throw new Error(`Downloaded block has no coinbase at height ${height}`);
			}
			if (!equals(storedBlock.coinbaseTx.coinbase, coinbase)) {
				throw new Error(`Stored block hash does not match downloaded block hash at height ${height}`);
			} */

			console.log(`Stored block at height ${height} (${humanize(block.header.hash)})`);
		}
	}
}
