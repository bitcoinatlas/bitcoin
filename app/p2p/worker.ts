import { delay } from "@std/async";
import { equals } from "@std/bytes";
import { GENESIS_BLOCK_HEADER } from "~/chain/genesis.ts";
import { verifyProofOfWork, workFromHeader } from "~/chain/pow.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { WireBlock } from "~/codec/wire/WireBlock.ts";
import { WireBlockHeader } from "~/codec/wire/WireBlockHeader.ts";
import { WireBlockHeaders } from "~/codec/wire/WireBlockHeaders.ts";
import { BlockMessage } from "~/p2p/messages/Block.ts";
import { GetDataMessage, MSG_WITNESS_BLOCK } from "~/p2p/messages/GetData.ts";
import { GetHeadersMessage } from "~/p2p/messages/GetHeaders.ts";
import { HeadersMessage } from "~/p2p/messages/Headers.ts";
import { Peer, type PeerMessageEvent } from "~/p2p/Peer.ts";
import { PeerChain } from "~/p2p/PeerChain.ts";
import { FastUint8ArraySet } from "~/libs/collections/FastUint8ArraySet.ts";
import { Queue } from "~/libs/collections/Queue.ts";

/*
Protocol
  in:  start		{ data }		initial header state from main
       target		{ height }		download block bodies up to here (latest-wins)
       seek			{ height }		main already has bodies up to here; resume after
       blacklist	{ hashes }		reject these headers (future / 2nd verifier)
  out: headers		{ from, data }	new headers appended at height `from`
       reorg		{ from, data }	branch replaced from height `from`; target reset
       blocks		{ blocks }		raw block payloads concatenated, decoded one by one by main
*/

const PROTOCOL_VERSION = 70015;
const MAGIC = new Uint8Array([0xf9, 0xbe, 0xb4, 0xd9]); // mainnet
const P2P_PORT = 8333;

const PEER_SYNC_COOLDOWN = 10 * 60 * 1000;
const SYNC_POLL_INTERVAL = 10;

// ── block download / packing knobs ────────────────────────────────────────
const CHUNK_BYTE_BUDGET = 10 * 1024 * 1024; // ~10 MiB raw per chunk shipped to main
const DOWNLOAD_AHEAD = 500; // blocks kept pooled-or-in-flight ahead of the pack cursor
const DOWNLOAD_BATCH = 16; // hashes per getdata
const BLOCK_TIMEOUT_MS = 30_000; // drop an in-flight request after this so it retries
const DOWNLOAD_IDLE_MS = 50; // pause when the next block isn't here yet

const messageQueue = new Queue<{ name: string; data: any }>(1000);
const blacklist = new FastUint8ArraySet(); // block hashes the main told us to reject
const peers = new Set<Peer>();
let targetDownloadHeight = 0; // ship bodies up to here (latest-wins)
let bodySentHeight = 0; // highest body height already shipped to main (cursor)

let localChain = new PeerChain();
const lastPeerSync = new WeakMap<Peer, number>();

// ── block download state (persists across syncBlocks calls / chunks) ───────
const blockPool = new Map<number, Uint8Array>(); // height -> raw BlockMessage payload
const blockInFlight = new Map<number, { peer: Peer; at: number }>(); // height -> who + when
const blockUnlisten = new Map<Peer, () => void>(); // attached block listeners

let started = false;

self.addEventListener("message", (event: MessageEvent) => messageQueue.enqueue(event.data));

async function drainMessages(): Promise<void> {
	let message;
	while ((message = messageQueue.dequeue())) {
		if (!started) {
			if (message.name !== "start") {
				throw new Error("First message should always be 'start'");
			}
			await start(WireBlockHeaders.decodeValue(message.data));
			continue;
		}

		if (message.name === "blacklist") {
			blacklist.add(Bytes32.decodeValue(message.data));
			continue;
		}

		if (message.name === "target") {
			targetDownloadHeight = message.data;
			continue;
		}

		if (message.name === "seek") {
			bodySentHeight = message.data;
			continue;
		}
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
	startSyncBlocks(); // body downloader, runs continuously up to targetDownloadHeight
	started = true;
}

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
		const responsePromise = peer.expect(HeadersMessage);
		await peer.send(GetHeadersMessage, {
			version: PROTOCOL_VERSION,
			locators,
			stopHash: new Uint8Array(32),
		});
		const [{ headers }] = await responsePromise;

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
		if (reorgHeight != null) {
			// reorg resets the download: wait for main to re-send seek/target, and
			// drop pooled/in-flight bodies — their heights may be different blocks now
			targetDownloadHeight = 0;
			bodySentHeight = 0;
			blockPool.clear();
			blockInFlight.clear();
		}
		console.log(`chain updated. height=${localChain.height()} work=${localChain.cumulativeWork()}`);
	} else {
		console.log(`kept existing chain. height=${localChain.height()} work=${localChain.cumulativeWork()}`);
	}
}

function verifyAndPushHeaders(chain: PeerChain, headers: WireBlockHeader[]): { reorgHeight?: number; pushed: number } {
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

function buildLocators(chain: PeerChain): Uint8Array[] {
	const locators: Uint8Array[] = [];
	let step = 1;
	let index = chain.height();

	while (index >= 0) {
		locators.push(chain.at(index)!.header.hash());
		if (locators.length >= 10) step <<= 1;
		index -= step;
	}

	// Always include genesis
	const genesis = chain.at(0)!.header.hash();
	if (!equals(locators.at(-1)!, genesis)) {
		locators.push(genesis);
	}

	return locators;
}

// ── block bodies ───────────────────────────────────────────────────────────

/**
 * Continuous body downloader. Runs forever; each pass fills one chunk (or stops
 * early on target-reached / no-peers) and ships it. Idles between passes. The
 * download window persists in module state so it stays warm across chunks.
 */
async function startSyncBlocks(): Promise<void> {
	while (true) {
		if (Math.min(targetDownloadHeight, localChain.height()) > bodySentHeight) {
			try {
				await syncBlocks();
			} catch (err) {
				console.error("block sync pass failed:", err);
			}
		}
		await delay(SYNC_POLL_INTERVAL);
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
	let packHeight = bodySentHeight + 1;
	let rr = 0; // round-robin cursor across peers

	while (true) {
		const top = Math.min(targetDownloadHeight, localChain.height());
		if (packHeight > top) break; // target reached

		const live = [...peers].filter((p) => p.connected);
		if (live.length === 0) break; // no peers
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
			flushChunk(chunk, chunkLen, packHeight - 1);
			return;
		}

		if (payload.length > chunk.length) {
			// single block bigger than the whole budget: ship it raw on its own
			self.postMessage({ name: "blocks", data: payload }, [payload.buffer]);
			bodySentHeight = packHeight;
		} else {
			chunk.set(payload, chunkLen); // raw, back to back, no framing
			chunkLen += payload.length;
		}
		blockPool.delete(packHeight);
		packHeight++;
	}

	flushChunk(chunk, chunkLen, packHeight - 1); // target reached / no peers: ship the tail
}

function flushChunk(chunk: Uint8Array, chunkLen: number, upTo: number): void {
	if (chunkLen === 0) return;
	const packed = chunk.subarray(0, chunkLen);
	self.postMessage({ name: "blocks", data: packed }, [packed.buffer]);
	bodySentHeight = upTo;
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
			console.error("[blocks] getdata error:", e);
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

		let block;
		try {
			[block] = WireBlock.decode(msg.payload);
		} catch (e) {
			console.error("[blocks] decode error:", e);
			return;
		}

		// O(1) hash -> height via the chain index; ignore unsolicited / out-of-range
		const height = localChain.heightOf(block.header.hash());
		if (height === undefined) return;
		if (height <= bodySentHeight) return;
		if (height > Math.min(targetDownloadHeight, localChain.height())) return;
		if (blockPool.has(height)) return;

		blockPool.set(height, msg.payload);
		blockInFlight.delete(height);
	});
	blockUnlisten.set(peer, off);
}

/** Detach listeners for gone peers and release their in-flight reservations. */
function dropDisconnectedPeers(): void {
	for (const [peer, off] of blockUnlisten) {
		if (peer.connected) continue;
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
	for (const [height, info] of blockInFlight) {
		if (info.at <= cutoff || !info.peer.connected) blockInFlight.delete(height);
	}
}

async function _searchBlock(hash: Uint8Array) {
	for (const peer of peers) {
		const block = await peer.sendAndExpect({
			send: {
				type: GetDataMessage,
				data: { inventory: [{ type: MSG_WITNESS_BLOCK, hash }] },
			},
			receive: {
				type: BlockMessage,
				filter(data) {
					return equals(data.header.hash(), hash);
				},
			},
		}).catch((reason) => {
			console.error(reason);
			return null;
		});
		if (block) return block;
	}
}

while (true) {
	await delay(0);
	await drainMessages();
}
