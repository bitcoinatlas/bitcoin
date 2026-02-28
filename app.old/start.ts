import { hexToBytes } from "@noble/hashes/utils";
import { join } from "@std/path";
import { BASE_DATA_DIR } from "~/lib/constants.ts";
import { Version } from "~/lib/satoshi/p2p/messages/Version.ts";
import { ChainManager } from "./lib/chain/ChainManager.ts";
import { PeerManager } from "./lib/satoshi/p2p/PeerManager.ts";
import { GetAddrHandler } from "./lib/satoshi/p2p/handlers/GetAddrHandler.ts";
import { GetHeadersHandler } from "./lib/satoshi/p2p/handlers/GetHeadersHandler.ts";
import { InvHandler } from "./lib/satoshi/p2p/handlers/InvHandler.ts";
import { PingHandler } from "./lib/satoshi/p2p/handlers/PingHandler.ts";
import { SendCmpctHandler } from "./lib/satoshi/p2p/handlers/SendCmpctHandler.ts";
import { SendHeadersHandler } from "./lib/satoshi/p2p/handlers/SendHeadersHandler.ts";
import { VersionHandler } from "./lib/satoshi/p2p/handlers/VersionHandler.ts";

const BITCOIN_NETWORK_MAGIC = hexToBytes("f9beb4d9");

const SERVICES = {
	NETWORK: 0x01n,
	BLOOM: 0x04n,
	WITNESS: 0x08n,
	COMPACT_BLOCKS: 0x20n,
	NETWORK_LIMITED: 0x10n,
} as const;

const version: Version = {
	version: 70015,
	services: SERVICES.WITNESS,
	timestamp: BigInt(Math.floor(Date.now() / 1000)),
	recvServices: SERVICES.WITNESS,
	transServices: SERVICES.WITNESS,
	nonce: 987654321n,
	userAgent: "/BitcoinAtlas:0.0.1-preview.1/",
	startHeight: 0,
	relay: false,
};

const peerManager = new PeerManager({
	magic: BITCOIN_NETWORK_MAGIC,
	version,
	maxConnections: 1,
	seeds: [/* { seedHost: "dnsseed.bitcoin.dashjr.org", peerPort: 8333 } */],
	handlers: [
		VersionHandler,
		PingHandler,
		InvHandler,
		GetHeadersHandler,
		SendHeadersHandler,
		SendCmpctHandler,
		GetAddrHandler,
	],
});

const chainManager = new ChainManager(join(BASE_DATA_DIR, "chain"), peerManager);

peerManager.addKnownPeer({ host: "192.168.1.10", port: 8333 });

await chainManager.init();
while (true) {
	await peerManager.maintainConnections();
	await chainManager.syncHeadersFromPeers(peerManager);
	await chainManager.downloadBlocks(10);

	// TODO: instead of chainManager doing everything, it can only have the header data and chain?
	// TODO: then we can have other stuff in here doing stuff based on its state and stuff.
	// TODO: also maybe dont prefix these with Chain, instead we can use HeaderManager or something?
	// TODO: also fix the issue with block download. lets make it run correctly first. then we can make it better.
	// TODO: height index doesnt have to be sequential right? we can have gaps.
	// TODO: how do we store blocks with gaps? current chunk system assumes sequential.
	// TODO: StoreBlock.fromBlock() shouldnt be there, its not just converting formats, it has pointer logic.
	// TODO: then the whole shit needs a refactor. mostly the `chain/` side
	// TODO: just make it make more sense as a codebase.
	// TODO: block download batching can be based on download size not block count. earlier blocks are way too small.
	// TODO: we need a general purpose walking abstraction for the chain. (download, verify hash, verify rules and utxos)
	// TODO: so basically 3 of these jobs should run in parallel independed from each other. but wait for each-other.
	// TODO: and download and verify hash should have a priority queue as well. so for example if we are showing the block in the explorer we should fetch it first.
	// TODO: if the priority queue is empty, it just uses the cursor or something. it needs to walk to chain find not downloaded or not verified blocks.
	// TODO: maybe we can store per block data on the header store? like for example the pointer for the actual block.
	// TODO: then the order we download the blocks doesnt matter because we know where each one is at.
	// TODO: but then we cant do a simple truncate if blocks are mixed and not ordered.
	// TODO: hmm i think we can still mix the blocks but then we need to order them while verifying sequentially?
	// TODO: so we know all the mixed chunks are invalid because they are higher than sorted height.
	// TODO: as we sequentially verify blocks, we can check if one is already downloaded in the mixed chunks.
	// TODO: if it is, we can just use that one and remove it from the mixed chunks. (but we cant really remove it from the mixed chunks easily)
	// TODO: hmm maybe idk. need to think about this more. either way we need two different storage types for blocks. (sorted and unsorted)
	// TODO: sorted ones are verified fully. unsorted ones are just downloaded and maybe verified partially (hash only).
	// TODO: since unsorted ones will be the ones that are downloaded on demand, there wont be many of them, so we dont have to chunk them.
	// TODO: we dont need height index, we can just store height based stuff inside the header store.
	// TODO: these are really simple things but code doesnt look simple, the code should reflect the simplicity of the idea.
}
