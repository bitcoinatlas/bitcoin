import { delay } from "@std/async";
import { equals } from "@std/bytes";
import { GENESIS_BLOCK_HASH, GENESIS_BLOCK_HEADER_DECODED, GENESIS_WORK } from "~/chain/genesis.ts";
import { verifyProofOfWork, workFromHeader } from "~/chain/pow.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { WireBlock } from "~/codec/wire/WireBlock.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { WireTxs } from "~/codec/wire/WireTxs.ts";
import { MAX_BLOCK_SIZE, MiB, MINUTE, SECOND } from "~/constants.ts";
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
import { PARALLELISM } from "~/env.ts";
import { verifySatoshiMerkleRoot } from "~/chain/merkle.ts";

const GENESIS_NODE: PeerChainNode = {
	header: GENESIS_BLOCK_HEADER_DECODED,
	cumulativeWork: GENESIS_WORK,
};

const PROTOCOL_VERSION = 70015;
const MAGIC = new Uint8Array([0xf9, 0xbe, 0xb4, 0xd9]); // mainnet
const P2P_PORT = 8333;

const PEER_SYNC_COOLDOWN = 20 * MINUTE;
const SYNC_POLL_INTERVAL = 10;

const BYTES_PER_ROUND_MIN = MAX_BLOCK_SIZE * PARALLELISM; // at least 1 block per worker

// ── memory model: two knobs, everything else derived ─────────────────────────
// 1. BYTES_PER_ROUND — block data committed per round (= PARALLELISM chunks, one
//    commit). Bounded by COMMIT cost, so FIXED — a bigger box must not mean
//    bigger commits.
// 2. LOOKAHEAD_FRACTION — share of RAM the download may run ahead. The elastic
//    part: more RAM → deeper buffer, no effect on commit size.
const BYTES_PER_ROUND = Math.max(64 * MiB, BYTES_PER_ROUND_MIN);
const LOOKAHEAD_FRACTION = 0.10; // one slice of RAM; block cache + memtables get theirs elsewhere

// per-worker chunk = a round split across the workers that process it.
const CHUNK_BYTE_BUDGET = Math.ceil(BYTES_PER_ROUND / PARALLELISM);

// look-ahead in blocks, bounded by MAX_BLOCK_SIZE so the raw pool can't exceed
// the RAM slice even if every block is max-sized.
const lookaheadBytes = Deno.systemMemoryInfo().total * LOOKAHEAD_FRACTION;
const BLOCK_DOWNLOAD_WINDOW = Math.max(PARALLELISM, Math.floor(lookaheadBytes / MAX_BLOCK_SIZE));

// keep ~2 rounds of finished chunks queued ahead of the consumers.
const MAX_QUEUED_ROUNDS = 2;

// ── p2p protocol mechanics (NOT memory — leave alone) ────────────────────────
const DOWNLOAD_BATCH = 16; // hashes per getdata (Core's value)
const MAX_BLOCKS_IN_TRANSIT_PER_PEER = 16 * 16; // per-peer send-queue cap (dev: 1 peer)
const BLOCK_TIMEOUT_MS = 30 * SECOND;
const DOWNLOAD_IDLE_MS = 50;

// Peers we should keep a connection to. Reconnected automatically with backoff
// when they drop, so a single transient disconnect never permanently stalls sync.
const PEER_ADDRESSES: { host: string; port: number }[] = [
	{ host: "192.168.8.10", port: P2P_PORT },
];
const RECONNECT_BASE_MS = 1 * SECOND; // first retry delay
const RECONNECT_MAX_MS = 30 * SECOND; // cap on exponential backoff

const messageQueue = new Queue<{ type: string; data: any }>(1000);
const blacklist = new FastUint8ArraySet(); // block hashes the main told us to reject
const peers = new Set<Peer>();

let cursor = 0; // download blocks after height (cursor)
let postedChunks = 0;
let consumedChunks = 0;

function keepDownloading() {
	return (postedChunks - consumedChunks) < PARALLELISM * MAX_QUEUED_ROUNDS;
}

let localChain = new PeerChain([GENESIS_NODE]);
const lastPeerSync = new WeakMap<Peer, number>();

const blockPool = new Map<number, Uint8Array>(); // height -> raw BlockMessage payload
const blockInFlight = new Map<number, { peer: Peer; at: number }>(); // height -> who + when
const blockUnlisten = new Map<Peer, () => void>(); // attached block listeners
const lastBlockAt = new Map<Peer, number>(); // peer -> last time it delivered a wanted block (liveness)

let started = false;
let lastSyncBlocksDiag = 0; // throttle gate for syncBlocks state diag

let port!: MessagePort;
self.addEventListener("message", (event) => {
	port = event.ports[0]!;
	port.addEventListener("message", (event) => messageQueue.enqueue(event.data));
	port.start();
}, { once: true });
self.postMessage(null);

async function drainMessages(): Promise<void> {
	let message;
	while ((message = messageQueue.dequeue())) {
		if (!started && message.type === "start") {
			const [headers] = WireBlockHeaders.decode(message.data);
			await start(headers);
			continue;
		}

		if (message.type === "blacklist") {
			const [hash] = Bytes32.decode(message.data);
			blacklist.add(hash);
			continue;
		}

		if (message.type === "consume") {
			consumedChunks++;
			console.log(`[p2p] consumed=${consumedChunks} waiting=${postedChunks - consumedChunks}`);
			continue;
		}

		if (message.type === "seek") {
			cursor = message.data;
			continue;
		}
	}
}

async function start(headers: WireBlockHeader[]) {
	console.log(`[p2p] start: ${headers.length} headers from main`);
	if (!headers.length) {
		port.postMessage({ type: "headers", data: WireBlockHeaders.encode([GENESIS_NODE.header]) });
	} else {
		headers.shift();
	}

	const { pushed } = verifyAndPushHeaders(localChain, headers);
	if (pushed !== headers.length) {
		console.error([
			`[p2p] was only able to verify loaded headers fully length=${headers.length} verified=${pushed}.`,
			`[p2p] this requires a reorg, but this happed while loading the data from disk, which is weird.`,
		].join("\n"));
		const keepHeight = Math.max(0, pushed - 1);
		if (localChain.height() !== keepHeight) {
			localChain.reorg(keepHeight);
		}
		port.postMessage({ type: "reorg", data: keepHeight });
	}

	// Connect to every configured peer, each with its own self-healing reconnect
	// loop. Wait for at least one to come up before starting the sync loops so the
	// first header pass has something to talk to.
	const firstConnects = PEER_ADDRESSES.map((addr) => connectAndMaintain(addr));
	await Promise.race(firstConnects).catch(() => {});

	startSyncHeaders();
	startSyncBlocks();
	started = true;
}

/**
 * Maintain a live connection to one peer address forever. On every disconnect
 * (or failed connect) it removes the dead Peer from the working set, waits with
 * exponential backoff, and dials again — so losing a peer is a transient blip,
 * not a permanent stall. Resolves the FIRST time the peer is connected+handshaked
 * (so start() can proceed); the maintenance loop keeps running after that.
 */
async function connectAndMaintain(addr: { host: string; port: number }): Promise<void> {
	let backoff = RECONNECT_BASE_MS;
	let signalledUp = false;
	let resolveUp!: () => void;
	const up = new Promise<void>((r) => (resolveUp = r));

	(async () => {
		while (true) {
			const peer = new Peer(addr.host, addr.port, MAGIC);
			try {
				await peer.connect();
				await handshake(peer);
				peers.add(peer);
				backoff = RECONNECT_BASE_MS; // reset on a good connection
				console.log(`[p2p] connected ${addr.host}:${addr.port}`);
				if (!signalledUp) {
					signalledUp = true;
					resolveUp();
				}

				// Block until this peer drops, then fall through to reconnect.
				await new Promise<void>((resolve) => {
					const off = peer.onDisconnect(() => {
						off();
						resolve();
					});
					// Guard against a race where it disconnected before the listener attached.
					if (!peer.connected) {
						off();
						resolve();
					}
				});

				console.log(`[p2p] peer ${addr.host}:${addr.port} dropped, reconnecting`);
			} catch (err) {
				console.error(`[p2p] connect ${addr.host}:${addr.port} failed:`, err);
			} finally {
				// Clean up dead peer + its download reservations regardless of how it died.
				peers.delete(peer);
				const off = blockUnlisten.get(peer);
				if (off) {
					off();
					blockUnlisten.delete(peer);
				}
				lastBlockAt.delete(peer);
				for (const [height, info] of blockInFlight) {
					if (info.peer === peer) blockInFlight.delete(height);
				}
			}

			await delay(backoff);
			backoff = Math.min(backoff * 2, RECONNECT_MAX_MS);
		}
	})();

	return up;
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
		// The reconnect manager (connectAndMaintain) owns removal + cleanup now;
		// just skip a dead peer here so we don't double-handle it.
		return;
	}
	const lastSync = lastPeerSync.get(peer) ?? 0;
	if (Date.now() - lastSync < PEER_SYNC_COOLDOWN) return;
	console.log("[p2p] syncing headers with peer");
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
		console.log(`[p2p] updating chain: height ${localChain.height()} -> ${peerChain.length() - 1}`);
		if (reorgHeight != null) {
			port.postMessage({ type: "reorg", data: reorgHeight });
			console.log(`[p2p] posted reorg keepHeight=${reorgHeight}`);
			const reorgLength = reorgHeight + 1;
			// TODO: you might probably just slice the message buffer instead
			const newHeaders = WireBlockHeaders.encode(peerChain.values().drop(reorgLength).map((node) => node.header).toArray());
			port.postMessage({ type: "headers", data: newHeaders }, [newHeaders.buffer]);
			console.log(`[p2p] posted ${peerChain.length() - reorgLength} headers after reorg`);
		} else {
			const newHeaders = WireBlockHeaders.encode(peerChain.values().drop(localChain.length()).map((node) => node.header).toArray());
			port.postMessage({ type: "headers", data: newHeaders }, [newHeaders.buffer]);
			console.log(`[p2p] posted ${peerChain.length() - localChain.length()} headers`);
		}

		localChain = peerChain;
		if (reorgHeight != null) {
			postedChunks = 0;
			consumedChunks = 0;
			cursor = 0;
			blockPool.clear();
			blockInFlight.clear();
			console.log("[p2p] cleared download state after reorg");
		}
		console.log(`[p2p] chain updated. height=${localChain.height()} work=${localChain.cumulativeWork()}`);
	} else {
		console.log(`[p2p] kept existing chain. height=${localChain.height()} work=${localChain.cumulativeWork()}`);
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
		console.log(`[p2p] reorging chain to height ${reorgHeight}`);
	}

	let pushed = 0;
	let prevHash = tip.header.hash();
	let cumulativeWork = tip.cumulativeWork;

	for (const header of headers) {
		if (blacklist.has(header.hash())) {
			console.log("[p2p] header in blacklist, stopping");
			break;
		}
		if (!equals(header.prevHash, prevHash)) {
			console.log("[p2p] header prevHash mismatch, stopping");
			break;
		}
		if (!verifyProofOfWork(header)) {
			console.log("[p2p] PoW verification failed, stopping");
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
	console.log("[p2p] block sync loop starting");
	while (true) {
		try {
			if (!keepDownloading()) {
				await delay(100);
				continue;
			}
			await syncBlocks();
		} catch (reason) {
			console.error("[p2p] block sync pass failed:", reason);
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
			console.log("[p2p] syncBlocks no live peers");
			break; // no peers
		}
		for (const peer of live) ensureBlockListener(peer);

		// keep the look-ahead window full, fanning batches across peers.
		// global room caps total in-flight (BLOCK_DOWNLOAD_WINDOW); the per-peer cap
		// (MAX_BLOCKS_IN_TRANSIT_PER_PEER, inside requestBlocks) caps any single peer's
		// send-queue depth so its blocks drain before the timeout.
		const room = BLOCK_DOWNLOAD_WINDOW - (blockPool.size + blockInFlight.size);
		if (room > 0) {
			rr = await requestBlocks(live, packHeight, top, room, rr);
		}

		const now0 = Date.now();
		if (now0 - lastSyncBlocksDiag > 10 * SECOND) {
			lastSyncBlocksDiag = now0;
			console.log(
				`[p2p] syncBlocks live=${live.length} packHeight=${packHeight} top=${top} inFlight=${blockInFlight.size} pool=${blockPool.size} room=${room}`,
			);
		}

		const payload = blockPool.get(packHeight);
		if (!payload) {
			// Head-of-line recovery. We pack strictly in order, so packHeight blocks
			// everything. It MUST always be in-flight with a fresh request — independent
			// of window room or peer liveness. Otherwise a single dropped delivery wedges
			// the whole download forever: the look-ahead pool fills, room hits 0 so the
			// normal path re-requests nothing, and the liveness reaper won't touch it
			// because the peer keeps delivering the *other* (higher) blocks.
			ensureHeadRequested(live, packHeight, rr);
			reapTimedOut();
			await delay(DOWNLOAD_IDLE_MS);
			continue;
		}

		// chunk full: ship it and return — driver re-enters for the next chunk.
		// (the current block stays pooled; next pass starts on it.)
		if (chunkLen > 0 && chunkLen + payload.length > CHUNK_BYTE_BUDGET) {
			console.log(`[p2p] chunk full at packHeight=${packHeight} len=${chunkLen}`);
			break;
		}

		if (payload.length > chunk.length) {
			// single block bigger than the whole budget: ship it raw on its own
			console.log(`[p2p] oversized block at height=${packHeight} size=${payload.length}`);
			port.postMessage({ type: "blocks", data: payload }, [payload.buffer]);
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
	console.log(`[p2p] post upTo=${packHeight - 1} size=${chunkLen}`);
	port.postMessage({ type: "blocks", data: packed }, [packed.buffer]);
	postedChunks++;
	cursor = packHeight - 1;
}

/** Count blocks currently reserved against a given peer. O(in-flight), which stays ≤ window size. */
function peerInFlight(peer: Peer): number {
	let n = 0;
	for (const info of blockInFlight.values()) {
		if (info.peer === peer) n++;
	}
	return n;
}

/**
 * Reserve and request the next blocks we don't already have/aren't fetching,
 * spreading DOWNLOAD_BATCH-sized batches across `live` peers round-robin. Each
 * peer's outstanding count is hard-capped at MAX_BLOCKS_IN_TRANSIT_PER_PEER (16,
 * Core's value) regardless of peer count or window size, so no single connection's
 * send queue grows deep enough to outrun the timeout. The global BLOCK_DOWNLOAD_WINDOW
 * caps total in-flight across all peers. The whole want-set is reserved in blockInFlight
 * BEFORE any await so the listener can't shrink the frontier mid-pass and collapse batches.
 */
async function requestBlocks(live: Peer[], fromHeight: number, top: number, globalRoom: number, rr: number): Promise<number> {
	// Core's rule: a hard cap of MAX_BLOCKS_IN_TRANSIT_PER_PEER outstanding per peer,
	// independent of peer count or window size. This is what keeps any one connection's
	// send queue shallow enough to drain before the timeout — the whole point of the fix.
	const perPeerCap = MAX_BLOCKS_IN_TRANSIT_PER_PEER;

	// spare capacity per peer, bounded by the global window
	const spare = new Map<Peer, number>();
	let totalSpare = 0;
	for (const peer of live) {
		const s = perPeerCap - peerInFlight(peer);
		if (s > 0) {
			spare.set(peer, s);
			totalSpare += s;
		}
	}
	totalSpare = Math.min(totalSpare, globalRoom);
	if (totalSpare <= 0) return rr;

	const want: { height: number; hash: Uint8Array }[] = [];
	for (let h = fromHeight; h <= top && want.length < totalSpare; h++) {
		if (blockPool.has(h) || blockInFlight.has(h)) continue;
		const node = localChain.at(h);
		if (!node) break;
		want.push({ height: h, hash: node.header.hash() });
	}
	if (want.length === 0) return rr;

	const now = Date.now();
	let i = 0;
	while (i < want.length) {
		// pick the next round-robin peer that still has spare capacity
		let peer: Peer | undefined;
		for (let tries = 0; tries < live.length; tries++) {
			const cand = live[rr++ % live.length]!;
			if ((spare.get(cand) ?? 0) > 0) {
				peer = cand;
				break;
			}
		}
		if (!peer) break; // every peer at its cap

		const take = Math.min(DOWNLOAD_BATCH, spare.get(peer)!, want.length - i);
		const batch = want.slice(i, i + take);
		i += take;
		spare.set(peer, spare.get(peer)! - take);
		for (const w of batch) blockInFlight.set(w.height, { peer, at: now }); // reserve before await
		try {
			await peer.send(GetDataMessage, {
				inventory: batch.map((w) => ({ type: MSG_WITNESS_BLOCK, hash: w.hash })),
			});
		} catch (e) {
			console.error("[p2p] getdata error:", e);
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
			console.error("[p2p] block decode error:", e);
			return;
		}

		if (!verifySatoshiMerkleRoot(block.txs, block.header.merkleRoot)) {
			// TODO: Handle this better later.
			throw new Error("Invalid merkle root");
		}

		// O(1) hash -> height via the chain index; ignore unsolicited / out-of-range
		const height = localChain.heightOf(block.header.hash());
		if (height === undefined) return;
		if (height <= cursor) return;
		if (blockPool.has(height)) return;

		blockInFlight.delete(height);
		lastBlockAt.set(peer, Date.now()); // proof this peer is alive and draining its queue
		blockPool.set(height, WireTxs.encode(block.txs));
	});
	blockUnlisten.set(peer, off);
}

/** Detach listeners for gone peers and release their in-flight reservations. */
function dropDisconnectedPeers(): void {
	for (const [peer, off] of blockUnlisten) {
		if (peer.connected) continue;
		console.log("[p2p] dropping disconnected peer listener");
		off();
		blockUnlisten.delete(peer);
		lastBlockAt.delete(peer);
	}
	for (const [height, info] of blockInFlight) {
		if (!info.peer.connected) blockInFlight.delete(height); // re-requested next pass
	}
}

/**
 * Guarantee the block we're blocked on is (re)requested. Unlike the look-ahead
 * window — which relies on per-peer liveness so deep, actively-draining queues
 * don't snowball — the head-of-line block gets a hard per-request timeout and
 * bypasses the window cap entirely. If it isn't in-flight, or its request is
 * older than BLOCK_TIMEOUT_MS (i.e. it was dropped/lost), re-reserve it against
 * the next live peer and fire a single-block getdata. Fire-and-forget so a
 * stuck send can't freeze the pack loop.
 */
function ensureHeadRequested(live: Peer[], height: number, rr: number): void {
	if (live.length === 0) return;
	const inFlight = blockInFlight.get(height);
	const now = Date.now();
	if (inFlight && now - inFlight.at <= BLOCK_TIMEOUT_MS) return; // still pending, give it time

	const node = localChain.at(height);
	if (!node) return;
	const peer = live[rr % live.length]!;
	blockInFlight.set(height, { peer, at: now }); // reserve (or refresh) the head
	peer
		.send(GetDataMessage, { inventory: [{ type: MSG_WITNESS_BLOCK, hash: node.header.hash() }] })
		.catch((e) => {
			console.error("[p2p] head-of-line getdata error:", e);
			blockInFlight.delete(height); // let the next pass retry
		});
}

/**
 * Release in-flight requests belonging to peers that have gone silent (or
 * dropped), so they retry elsewhere. Liveness is judged per *peer*, not per
 * request: a peer steadily delivering blocks keeps all of its reservations no
 * matter how deep its send queue is — which is exactly what lets one peer hold
 * a large window without the old send-time clock reaping blocks it simply
 * hasn't reached yet. A peer that never delivers falls back to its first
 * request time, so a truly dead peer is still reaped after BLOCK_TIMEOUT_MS.
 * Recovery of a single lost block at the pack frontier is NOT this function's
 * job — that's ensureHeadRequested, which doesn't depend on peer silence.
 */
function reapTimedOut(): void {
	const now = Date.now();
	let reaped = 0;
	for (const [height, info] of blockInFlight) {
		// HARD GRACE: never reap a reservation younger than BLOCK_TIMEOUT_MS, no matter
		// what. A request made seconds ago is never legitimately "dead" — the block may
		// still be in flight, and a peer-object swap (reconnect) must not nuke fresh
		// reservations. Without this, a reconnect cycle reaps the whole just-issued window
		// and the pack loop wedges: re-request → reaped → re-request, head never lands.
		if (now - info.at < BLOCK_TIMEOUT_MS) continue;

		// Past the grace window: reap if the peer is gone, or it delivered before and then
		// went silent. (everDelivered guards the startup case where a warming-up peer that
		// hasn't delivered yet shouldn't have its window swept.)
		const dead = !info.peer.connected || !peers.has(info.peer);
		const everDelivered = lastBlockAt.has(info.peer);
		const silent = everDelivered && now - lastBlockAt.get(info.peer)! > BLOCK_TIMEOUT_MS;
		if (dead || silent) {
			blockInFlight.delete(height);
			reaped++;
		}
	}
	if (reaped > 0) console.log(`[p2p] reaped ${reaped} requests from silent/dropped peers`);
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
