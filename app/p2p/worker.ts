import { delay } from "@std/async";
import { equals } from "@std/bytes";
import { GENESIS_BLOCK_HEADER } from "~/chain/utils/genesis.ts";
import { verifyProofOfWork, workFromHeader } from "~/chain/utils/pow.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { Peer } from "~/p2p/Peer.ts";
import { PeerChain } from "~/p2p/PeerChain.ts";
import { buildLocators, getHeaders } from "~/p2p/utils/headers.ts";
import { FastUint8ArraySet } from "~/utils/FastUint8ArraySet.ts";
import { Queue } from "~/utils/Queue.ts";

/*
Protocol
  in:  start		{ data }		initial header state from main
       target		{ height }		download block bodies up to here (latest-wins)
       seek			{ height }		main already has bodies up to here; resume after
       blacklist	{ hashes }		reject these headers (future / 2nd verifier)
  out: headers		{ from, data }	new headers appended at height `from`
       reorg		{ from, data }	branch replaced from height `from`; target reset
       blocks		{ blocks }		packaged buffer of blocks (txs)
*/

const MAGIC = new Uint8Array([0xf9, 0xbe, 0xb4, 0xd9]); // mainnet
const P2P_PORT = 8333;

let localChain = new PeerChain();
const peers = new Set<Peer>();

const messageQueue = new Queue<{ name: string; data: any }>(1000);
const blacklist = new FastUint8ArraySet(); // block hashes the main told us to reject
let started = false;

self.addEventListener("message", (event: MessageEvent) => messageQueue.enqueue(event.data));

async function drainMessages(): Promise<void> {
	let message;
	while ((message = messageQueue.dequeue())) {
		if (!started) {
			if (message.name === "start") {
				await start(WireBlockHeaders.decodeValue(message.data));
			}
			continue; // deaf to everything but `start` until started
		}

		if (message.name === "blacklist") {
			blacklist.add(Bytes32.decodeValue(message.data));
			continue;
		}

		// TODO: target / seek
	}
}

async function start(headers: WireBlockHeader[]) {
	if (!headers.length) {
		verifyAndPushHeaders(localChain, [WireBlockHeader.decodeValue(GENESIS_BLOCK_HEADER)]);
	} else {
		const { pushed } = verifyAndPushHeaders(localChain, headers);
		if (pushed !== headers.length) {
			console.error([
				`was only able to verify loaded headers fully length=${headers.length} verified=${pushed}.`,
				`this requires a reorg, but this happed while loading the data from disk, which is weird.`,
			].join("\n"));
			const keepHeight = pushed - 1;
			if (localChain.height() !== keepHeight) {
				localChain.reorg(keepHeight);
			}
			self.postMessage({ name: "reorg", data: keepHeight });
		}
	}

	const devPeer = new Peer(`192.168.8.10`, P2P_PORT, MAGIC);
	await devPeer.connect();
	peers.add(devPeer);

	startSyncPeers(); // fire-and-forget; keeps the worker responsive during IBD
	started = true;
}

const PEER_SYNC_COOLDOWN = 10 * 60 * 1000;
const SYNC_POLL_INTERVAL = 1000;
const lastPeerSync = new WeakMap<Peer, number>();

async function startSyncPeers(): Promise<void> {
	while (true) {
		try {
			await syncPeers();
		} catch (err) {
			console.error("peer sync pass failed:", err);
		}
		await delay(SYNC_POLL_INTERVAL); // idle between passes; cooldown handled per-peer
	}
}

async function syncPeers() {
	for (const peer of peers) {
		if (!peer.connected) {
			peers.delete(peer);
			continue;
		}
		const lastSync = lastPeerSync.get(peer) ?? 0;
		if (Date.now() - lastSync < PEER_SYNC_COOLDOWN) continue; // skip this peer, not the rest
		lastPeerSync.set(peer, Date.now());
		await syncPeerHeaders(peer);
	}
}

async function syncPeerHeaders(peer: Peer) {
	const peerChain = new PeerChain(localChain.values().toArray());
	const locators = buildLocators(peerChain);
	let reorgHeight: number | undefined;

	while (true) {
		const headers = await getHeaders(peer, locators);
		if (headers.length === 0) break; // peer at tip

		const result = verifyAndPushHeaders(peerChain, headers);
		if (result.reorgHeight != null && (reorgHeight == null || result.reorgHeight < reorgHeight)) {
			reorgHeight = result.reorgHeight;
		}
		if (result.pushed === 0) break; // no new valid blocks, treat as tip

		// Next request: single locator = current tip
		const newTip = peerChain.tip()!;
		locators.length = 0;
		locators.push(newTip.header.hash());
	}

	// commit only if cumulative work improved
	if (peerChain.cumulativeWork() > localChain.cumulativeWork()) {
		console.log(`Updating chain: height ${localChain.height()} -> ${peerChain.length() - 1}`);
		if (reorgHeight != null) {
			self.postMessage({ name: "reorg", data: reorgHeight });
			const reorgLength = reorgHeight + 1;
			// TODO: you might probably just slice the message buffer instead
			const newHeaders = WireBlockHeaders.encode(peerChain.values().drop(reorgLength).map((node) => node.header).toArray());
			self.postMessage({ name: "headers", data: newHeaders }, [newHeaders.buffer]);
		} else {
			const newHeaders = WireBlockHeaders.encode(peerChain.values().drop(localChain.length()).map((node) => node.header).toArray());
			self.postMessage({ name: "headers", data: newHeaders }, [newHeaders.buffer]);
		}

		localChain = peerChain;
		console.log(`chain updated. height=${localChain.height()} work=${localChain.cumulativeWork()}`);
	} else {
		console.log(`kept existing chain. height=${localChain.height()} work=${localChain.cumulativeWork()}`);
	}
}

export function verifyAndPushHeaders(chain: PeerChain, headers: WireBlockHeader[]): { reorgHeight?: number; pushed: number } {
	const head = headers.at(0)!;
	let tip = chain.tip()!;
	let reorgHeight: number | undefined;

	if (!equals(tip.header.hash(), head.prevHash)) {
		reorgHeight = chain.heightOf(head.prevHash) ?? 0;
		chain.reorg(reorgHeight);
		tip = chain.tip()!;
		console.log(`reorging chain to height ${reorgHeight}`);
	}

	let pushed = 0;
	let prevHash = tip.header.hash();
	let cumulativeWork = tip.cumulativeWork;

	for (const header of headers) {
		if (blacklist.has(header.hash())) break;
		if (!equals(header.prevHash, prevHash)) break; // chain onto current tip
		if (!verifyProofOfWork(header)) break;

		cumulativeWork += workFromHeader(header);
		chain.push({ header, cumulativeWork });

		prevHash = header.hash();
		pushed++;
	}

	return { pushed, reorgHeight };
}

while (true) {
	await delay(0);
	await drainMessages();
}
