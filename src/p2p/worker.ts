import { delay } from "@std/async";
import { equals } from "@std/bytes";
import { GENESIS_BLOCK_HASH, GENESIS_BLOCK_HEADER, GENESIS_WORK } from "~/chain/genesis.ts";
import { verifyProofOfWork, workFromHeader } from "~/chain/pow.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { WireBlock } from "~/codec/wire/WireBlock.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { MAX_BLOCK_SIZE } from "~/constants.ts";
import { FastUint8ArraySet } from "~/libs/collections/FastUint8ArraySet.ts";
import { Queue } from "~/libs/collections/Queue.ts";
import { BlockMessage } from "~/p2p/messages/Block.ts";
import { GetDataMessage, MSG_WITNESS_BLOCK } from "~/p2p/messages/GetData.ts";
import { GetHeadersMessage } from "~/p2p/messages/GetHeaders.ts";
import { HeadersMessage } from "~/p2p/messages/Headers.ts";
import { Peer, type PeerMessageEvent } from "~/p2p/Peer.ts";
import { PeerChain } from "~/p2p/PeerChain.ts";
import { PeerChainNode } from "~/p2p/PeerChainNode.ts";
import { handshake } from "~/p2p/peers.ts";

const GENESIS_NODE: PeerChainNode = {
	header: WireBlockHeader.decodeValue(GENESIS_BLOCK_HEADER),
	cumulativeWork: GENESIS_WORK,
};

const PROTOCOL_VERSION = 70015;
const MAGIC = new Uint8Array([0xf9, 0xbe, 0xb4, 0xd9]); // mainnet
const P2P_PORT = 8333;

const PEER_SYNC_COOLDOWN = 20 * 60 * 1000;
const SYNC_POLL_INTERVAL = 10;

const CHUNK_BYTE_BUDGET = 20 * MAX_BLOCK_SIZE;
const DOWNLOAD_AHEAD = 500; // blocks kept pooled-or-in-flight ahead of the pack cursor
const DOWNLOAD_BATCH = 16; // hashes per getdata
const BLOCK_TIMEOUT_MS = 30_000; // drop an in-flight request after this so it retries
const DOWNLOAD_IDLE_MS = 50; // pause when the next block isn't here yet

const messageQueue = new Queue<{ name: string; data: any }>(1000);
const blacklist = new FastUint8ArraySet(); // block hashes the main told us to reject
const peers = new Set<Peer>();

let cursor = 0; // download blocks after height (cursor)
let postedChunks = 0;
let consumedChunks = 0;

function keepDownloading() {
	const notConsumed = postedChunks - consumedChunks;
	return notConsumed < 10;
}

let localChain = new PeerChain([GENESIS_NODE]);
const lastPeerSync = new WeakMap<Peer, number>();

const blockPool = new Map<number, Uint8Array>(); // height -> raw BlockMessage payload
const blockInFlight = new Map<number, { peer: Peer; at: number }>(); // height -> who + when
const blockUnlisten = new Map<Peer, () => void>(); // attached block listeners

let started = false;

self.addEventListener("message", (event: MessageEvent) => messageQueue.enqueue(event.data));

async function drainMessages(): Promise<void> {
	let message;
	while ((message = messageQueue.dequeue())) {
		if (!started && message.name === "start") {
			await start(WireBlockHeaders.decodeValue(message.data));
			continue;
		}

		if (message.name === "blacklist") {
			blacklist.add(Bytes32.decodeValue(message.data));
			continue;
		}

		if (message.name === "consume") {
			consumedChunks++;
			console.log(`[worker] consumed=${consumedChunks} waiting=${postedChunks - consumedChunks}`);
			continue;
		}

		if (message.name === "seek") {
			cursor = message.data;
			continue;
		}
	}
}

async function start(headers: WireBlockHeader[]) {
	console.log(`[worker] start: ${headers.length} headers from main`);
	if (!headers.length) {
		self.postMessage({ name: "headers", data: WireBlockHeaders.encode([GENESIS_NODE.header]) });
	} else {
		headers.shift();
	}

	const { pushed } = verifyAndPushHeaders(localChain, headers);
	if (pushed !== headers.length) {
		console.error([
			`[worker] was only able to verify loaded headers fully length=${headers.length} verified=${pushed}.`,
			`[worker] this requires a reorg, but this happed while loading the data from disk, which is weird.`,
		].join("\n"));
		const keepHeight = Math.max(0, pushed - 1);
		if (localChain.height() !== keepHeight) {
			localChain.reorg(keepHeight);
		}
		self.postMessage({ name: "reorg", data: keepHeight });
	}

	const devPeer = new Peer(`192.168.8.10`, P2P_PORT, MAGIC);
	await devPeer.connect();
	await handshake(devPeer);
	peers.add(devPeer);

	startSyncHeaders();
	startSyncBlocks();
	started = true;
}

async function startSyncHeaders(): Promise<void> {
	while (true) {
		for (const peer of peers) {
			try {
				await syncHeaders(peer);
			} catch (err) {
				console.error("peer sync pass failed:", err);
			}
		}
		await delay(SYNC_POLL_INTERVAL);
	}
}

async function syncHeaders(peer: Peer) {
	if (!peer.connected) {
		console.log("[worker] peer disconnected, removing");
		peers.delete(peer);
		return;
	}
	const lastSync = lastPeerSync.get(peer) ?? 0;
	if (Date.now() - lastSync < PEER_SYNC_COOLDOWN) return;
	console.log("[worker] syncing headers with peer");
	lastPeerSync.set(peer, Date.now());

	const peerChain = new PeerChain(localChain.values());
	const locators = buildLocators(peerChain);
	let reorgHeight: number | undefined;

	while (true) {
		const responsePromise = peer.expect(HeadersMessage);
		await peer.send(GetHeadersMessage, { version: PROTOCOL_VERSION, locators, stopHash: new Uint8Array(32) });
		const [{ headers }] = await responsePromise;

		if (headers.length === 0) break; // peer at tip

		const result = verifyAndPushHeaders(peerChain, headers);
		if (result.reorgHeight != null && (reorgHeight == null || result.reorgHeight < reorgHeight)) {
			reorgHeight = result.reorgHeight;
		}
		if (result.pushed === 0) break; // no new valid blocks, treat as tip

		// next
		const newTip = peerChain.tip()!;
		locators.length = 0;
		locators.push(newTip.header.hash());
	}

	if (peerChain.cumulativeWork() > localChain.cumulativeWork()) {
		console.log(`[worker] updating chain: height ${localChain.height()} -> ${peerChain.length() - 1}`);
		if (reorgHeight != null) {
			self.postMessage({ name: "reorg", data: reorgHeight });
			console.log(`[worker] posted reorg keepHeight=${reorgHeight}`);
			const reorgLength = reorgHeight + 1;
			// TODO: you might probably just slice the message buffer instead
			const newHeaders = WireBlockHeaders.encode(peerChain.values().drop(reorgLength).map((node) => node.header).toArray());
			self.postMessage({ name: "headers", data: newHeaders }, [newHeaders.buffer]);
			console.log(`[worker] posted ${peerChain.length() - reorgLength} headers after reorg`);
		} else {
			const newHeaders = WireBlockHeaders.encode(peerChain.values().drop(localChain.length()).map((node) => node.header).toArray());
			self.postMessage({ name: "headers", data: newHeaders }, [newHeaders.buffer]);
			console.log(`[worker] posted ${peerChain.length() - localChain.length()} headers`);
		}

		localChain = peerChain;
		if (reorgHeight != null) {
			postedChunks = 0;
			consumedChunks = 0;
			cursor = 0;
			blockPool.clear();
			blockInFlight.clear();
			console.log("[worker] cleared download state after reorg");
		}
		console.log(`[worker] chain updated. height=${localChain.height()} work=${localChain.cumulativeWork()}`);
	} else {
		console.log(`[worker] kept existing chain. height=${localChain.height()} work=${localChain.cumulativeWork()}`);
	}
}

function verifyAndPushHeaders(chain: PeerChain, headers: WireBlockHeader[]): { reorgHeight?: number; pushed: number } {
	const head = headers.at(0);
	if (!head) return { pushed: 0 };
	let tip = chain.tip()!;

	let reorgHeight: number | undefined;

	if (!equals(tip.header.hash(), head.prevHash)) {
		reorgHeight = chain.heightOf(head.prevHash) ?? 0;
		chain.reorg(reorgHeight);
		tip = chain.tip()!;
		console.log(`[worker] reorging chain to height ${reorgHeight}`);
	}

	let pushed = 0;
	let prevHash = tip.header.hash();
	let cumulativeWork = tip.cumulativeWork;

	for (const header of headers) {
		if (blacklist.has(header.hash())) {
			console.log("[worker] header in blacklist, stopping");
			break;
		}
		if (!equals(header.prevHash, prevHash)) {
			console.log("[worker] header prevHash mismatch, stopping");
			break;
		}
		if (!verifyProofOfWork(header)) {
			console.log("[worker] PoW verification failed, stopping");
			break;
		}

		cumulativeWork += workFromHeader(header);
		chain.push({ header, cumulativeWork });

		prevHash = header.hash();
		pushed++;
	}

	return { pushed, reorgHeight };
}

async function startSyncBlocks(): Promise<void> {
	console.log("[worker] block sync loop starting");
	while (true) {
		try {
			if (!keepDownloading()) {
				await delay(100);
				continue;
			}
			await syncBlocks();
		} catch (reason) {
			console.error("[worker] block sync pass failed:", reason);
		} finally {
			await delay(SYNC_POLL_INTERVAL);
		}
	}
}

/**
 * Fill one chunk: download bodies in strict height order from `bodySentHeight + 1`,
 * write their raw payloads back-to-back into a CHUNK_BYTE_BUDGET buffer, and post
 * it to main. Fans getdata batches across all connected peers (round-robin) so a
 * slow peer doesn't hold up the rest. Returns when the chunk is full, the target
 * is reached, or there are no connected peers.
 */
async function syncBlocks(): Promise<void> {
	dropDisconnectedPeers();

	const chunk = new Uint8Array(CHUNK_BYTE_BUDGET);
	let chunkLen = 0;
	let packHeight = cursor + 1;
	let rr = 0; // round-robin cursor across peers

	while (true) {
		const top = localChain.height();
		if (packHeight > top) break;

		const live = peers.values().filter((p) => p.connected).toArray();
		if (live.length === 0) {
			console.log("[worker] syncBlocks no live peers");
			break; // no peers
		}
		for (const peer of live) ensureBlockListener(peer);

		// keep the look-ahead window full, fanning batches across peers
		const remaining = top - packHeight + 1;
		const room = DOWNLOAD_AHEAD - (blockPool.size + blockInFlight.size);
		if (room >= DOWNLOAD_BATCH || (remaining < DOWNLOAD_BATCH && room > 0)) {
			rr = await requestBlocks(live, packHeight, top, room, rr);
		}

		const payload = blockPool.get(packHeight);
		if (!payload) {
			reapTimedOut();
			await delay(DOWNLOAD_IDLE_MS);
			continue;
		}

		// chunk full: ship it and return — driver re-enters for the next chunk.
		// (the current block stays pooled; next pass starts on it.)
		if (chunkLen > 0 && chunkLen + payload.length > CHUNK_BYTE_BUDGET) {
			console.log(`[worker] chunk full at packHeight=${packHeight} len=${chunkLen}`);
			break;
		}

		if (payload.length > chunk.length) {
			// single block bigger than the whole budget: ship it raw on its own
			console.log(`[worker] oversized block at height=${packHeight} size=${payload.length}`);
			self.postMessage({ name: "blocks", data: payload }, [payload.buffer]);
			postedChunks++;
			cursor = packHeight;
		} else {
			chunk.set(payload, chunkLen); // raw, back to back, no framing
			chunkLen += payload.length;
		}
		blockPool.delete(packHeight);
		packHeight++;
	}

	if (chunkLen === 0) return;
	const packed = chunk.subarray(0, chunkLen);
	console.log(`[worker] post upTo=${packHeight - 1} size=${chunkLen}`);
	self.postMessage({ name: "blocks", data: packed }, [packed.buffer]);
	postedChunks++;
	cursor = packHeight - 1;
}

/**
 * Reserve and request the next blocks we don't already have/aren't fetching,
 * spreading DOWNLOAD_BATCH-sized batches across `live` peers round-robin. The
 * whole want-set is reserved in blockInFlight BEFORE any await so the listener
 * can't shrink the frontier mid-pass and collapse batches.
 */
async function requestBlocks(live: Peer[], fromHeight: number, top: number, room: number, rr: number): Promise<number> {
	const want: { height: number; hash: Uint8Array }[] = [];
	for (let h = fromHeight; h <= top && want.length < room; h++) {
		if (blockPool.has(h) || blockInFlight.has(h)) continue;
		const node = localChain.at(h);
		if (!node) break;
		want.push({ height: h, hash: node.header.hash() });
	}
	if (want.length === 0) return rr;

	const now = Date.now();
	for (let i = 0; i < want.length; i += DOWNLOAD_BATCH) {
		const peer = live[rr % live.length]!;
		rr++;
		const batch = want.slice(i, i + DOWNLOAD_BATCH);
		for (const w of batch) blockInFlight.set(w.height, { peer, at: now }); // reserve before await
		try {
			await peer.send(GetDataMessage, {
				inventory: batch.map((w) => ({ type: MSG_WITNESS_BLOCK, hash: w.hash })),
			});
		} catch (e) {
			console.error("[worker] getdata error:", e);
			for (const w of batch) blockInFlight.delete(w.height); // unsend so they retry elsewhere
		}
	}
	return rr;
}

/** Attach the persistent block listener to a peer (idempotent). Fills blockPool. */
function ensureBlockListener(peer: Peer): void {
	if (blockUnlisten.has(peer)) return;
	const off = peer.onMessage((msg: PeerMessageEvent) => {
		if (msg.command !== BlockMessage.command) return;

		let block: WireBlock;
		try {
			[block] = WireBlock.decode(msg.payload);
		} catch (e) {
			console.error("[worker] block decode error:", e);
			return;
		}

		// O(1) hash -> height via the chain index; ignore unsolicited / out-of-range
		const height = localChain.heightOf(block.header.hash());
		if (height === undefined) return;
		if (height <= cursor) return;
		if (blockPool.has(height)) return;

		blockInFlight.delete(height);
		blockPool.set(height, StoredTxs.encode(block.txs.map((tx) => StoredTx.fromWire(tx))));
	});
	blockUnlisten.set(peer, off);
}

/** Detach listeners for gone peers and release their in-flight reservations. */
function dropDisconnectedPeers(): void {
	for (const [peer, off] of blockUnlisten) {
		if (peer.connected) continue;
		console.log("[worker] dropping disconnected peer listener");
		off();
		blockUnlisten.delete(peer);
	}
	for (const [height, info] of blockInFlight) {
		if (!info.peer.connected) blockInFlight.delete(height); // re-requested next pass
	}
}

/** Release in-flight requests that timed out or whose peer dropped, so they retry. */
function reapTimedOut(): void {
	const cutoff = Date.now() - BLOCK_TIMEOUT_MS;
	let reaped = 0;
	for (const [height, info] of blockInFlight) {
		if (info.at <= cutoff || !info.peer.connected) {
			blockInFlight.delete(height);
			reaped++;
		}
	}
	if (reaped > 0) console.log(`[worker] reaped ${reaped} timed-out block requests`);
}

function buildLocators(chain: PeerChain): Uint8Array[] {
	const locators: Uint8Array[] = [];
	let step = 1;
	let index = chain.height();

	while (index > 0) {
		locators.push(chain.at(index)!.header.hash());
		if (locators.length >= 10) step <<= 1;
		index -= step;
	}

	const last = locators.at(-1);
	if (!last || !equals(last, GENESIS_BLOCK_HASH)) {
		locators.push(GENESIS_BLOCK_HASH);
	}

	return locators;
}

while (true) {
	await delay(10);
	await drainMessages();
}
