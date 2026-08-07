import { delay } from "@std/async";
import { equals } from "@std/bytes";
import { manifest } from "~/chain/manifest.ts";
import { GENESIS_BLOCK, GENESIS_BLOCK_HASH } from "~/chain/genesis.ts";
import { verifySatoshiMerkleRoot } from "~/chain/merkle.ts";
import { Bytes32 } from "@project/codecs";
import { WireBlock } from "@project/codecs";
import { WireBlockHeader } from "@project/codecs";
import { WireBlockHeaders } from "@project/codecs";
import { WireTxs } from "@project/codecs";
import { MAX_BLOCK_SIZE, MiB, MINUTE, SECOND } from "@project/utils";
import { BASE_DATA_DIR, PARALLELISM_THREADS } from "~/env.ts";
import { join } from "@std/path";
import { encodeHex } from "@std/encoding";
import { FastUint8ArraySet } from "@project/collections";
import { Queue } from "@project/collections";
import { BlockMessage } from "~/p2p/messages/Block.ts";
import { GetDataMessage, MSG_WITNESS_BLOCK } from "~/p2p/messages/GetData.ts";
import { GetHeadersMessage } from "~/p2p/messages/GetHeaders.ts";
import { HeadersMessage } from "~/p2p/messages/Headers.ts";
import { Peer, type PeerMessageEvent } from "~/p2p/Peer.ts";
import { handshake } from "~/p2p/peers.ts";

console.log("[p2p] booting");

// ── protocol / peers ─────────────────────────────────────────────────────────
const PROTOCOL_VERSION = 70015;
const MAGIC = new Uint8Array([0xf9, 0xbe, 0xb4, 0xd9]); // mainnet
const P2P_PORT = 8333;

const PEER_ADDRESSES: { host: string; port: number }[] = [
	{ host: "192.168.8.10", port: P2P_PORT },
];
const RECONNECT_BASE_MS = 1 * SECOND;
const RECONNECT_MAX_MS = 30 * SECOND;

const PEER_SYNC_COOLDOWN = 20 * MINUTE;
const SYNC_POLL_INTERVAL = 10;

// ── block-download memory model (two knobs, everything else derived) ──────────
// BYTES_PER_ROUND is fixed (bounded by commit cost); LOOKAHEAD_FRACTION is the
// elastic buffer that grows with RAM. Chunk = one round split across workers.
const BYTES_PER_ROUND_MIN = MAX_BLOCK_SIZE * PARALLELISM_THREADS;
const BYTES_PER_ROUND = Math.max(64 * MiB, BYTES_PER_ROUND_MIN);
const CHUNK_BYTE_BUDGET = Math.ceil(BYTES_PER_ROUND / PARALLELISM_THREADS);

const LOOKAHEAD_FRACTION = 0.10;
const lookaheadBytes = Deno.systemMemoryInfo().total * LOOKAHEAD_FRACTION;
const BLOCK_DOWNLOAD_WINDOW = Math.max(PARALLELISM_THREADS, Math.floor(lookaheadBytes / MAX_BLOCK_SIZE));
const MAX_QUEUED_ROUNDS = 2;

// ── p2p download mechanics (protocol, not memory — leave alone) ───────────────
const DOWNLOAD_BATCH = 16; // hashes per getdata (Core's value)
const MAX_BLOCKS_IN_TRANSIT_PER_PEER = 16 * 16;
const BLOCK_TIMEOUT_MS = 30 * SECOND;
const DOWNLOAD_IDLE_MS = 50;

// ── state ────────────────────────────────────────────────────────────────────
let port!: MessagePort;
const messageQueue = new Queue<{ type: string; data: any }>(1000);
// Persistent, append-only set of 32-byte header hashes main told us to reject.
// Crash-safe by construction: add() appends one 32-byte record and fsyncs
// before returning, open() ignores any torn trailing partial record on load.
// No delete — a ban is forever, the same contract as FastUint8ArraySet, which
// backs the in-memory lookups.
class PersistentHashSet {
	private static readonly KEY = 32;
	private readonly set = new FastUint8ArraySet();
	private readonly file: Deno.FsFile;

	private constructor(file: Deno.FsFile) {
		this.file = file;
	}

	public static open(path: string): PersistentHashSet {
		const file = Deno.openSync(path, { read: true, write: true, create: true });
		const self = new PersistentHashSet(file);
		const size = file.statSync().size;
		const whole = size - (size % PersistentHashSet.KEY); // drop any torn tail
		if (whole > 0) {
			const buf = new Uint8Array(whole);
			file.seekSync(0, Deno.SeekMode.Start);
			for (let read = 0; read < whole;) {
				const n = file.readSync(buf.subarray(read));
				if (n === null) break;
				read += n;
			}
			for (let off = 0; off < whole; off += PersistentHashSet.KEY) {
				self.set.add(buf.subarray(off, off + PersistentHashSet.KEY));
			}
		}
		file.seekSync(0, Deno.SeekMode.End);
		return self;
	}

	public has(hash: Uint8Array): boolean {
		return this.set.has(hash);
	}

	public add(hash: Uint8Array): void {
		if (this.set.has(hash)) return; // append-only: never write a duplicate
		for (let written = 0; written < hash.length;) written += this.file.writeSync(hash.subarray(written));
		this.file.syncSync(); // durable before we act on the ban
		this.set.add(hash);
	}
}

const blacklist = PersistentHashSet.open(join(BASE_DATA_DIR, "blacklist")); // header hashes main told us to reject
const peers = new Set<Peer>();
const lastPeerSync = new WeakMap<Peer, number>();

// Header apply is now the chain worker's job. p2p forwards a fetched batch and
// waits for chain's "headers-applied" ack before reading the new tip or asking
// for more — the ack means the header mmap is pinned and safe to read. Only one
// header batch is ever in flight (syncHeaders awaits each), so a single pending
// resolver is enough.
type ApplyResult = { adopted: number; rewind?: number };
let pendingHeaderApply: ((result: ApplyResult) => void) | undefined;

let started = false;
let cursor = 0; // download blocks after this height
let postedChunks = 0;
let consumedChunks = 0;
let lastSyncBlocksDiag = 0;

const blockPool = new Map<number, Uint8Array>(); // height -> raw block-body payload
const blockInFlight = new Map<number, { peer: Peer; at: number }>(); // height -> who + when
const blockUnlisten = new Map<Peer, () => void>(); // attached block listeners
const lastBlockAt = new Map<Peer, number>(); // peer -> last delivered-a-wanted-block time

self.onmessage = async (event) => {
	console.log("[p2p] main-port message event, ports:", event.ports.length);
	port = event.ports[0]!;
	port.addEventListener("message", (event) => messageQueue.enqueue(event.data));
	port.start();
	console.log("[p2p] sync port received");

	while (true) {
		await delay(10);
		await drainMessages();
	}
};

self.onunhandledrejection = (e) => {
	console.error("[p2p] unhandledrejection:", e.reason);
};

self.postMessage(null);

function keepDownloading(): boolean {
	return (postedChunks - consumedChunks) < PARALLELISM_THREADS * MAX_QUEUED_ROUNDS;
}

// ── header chain, read straight from mmap ─────────────────────────────────────
// The header ArrayStore + blockhash HashMapStore ARE the chain. No in-memory
// copy: p2p is the only worker touching this domain, so reads up to size() are
// always its own committed writes.

function tipHeight(): number {
	return manifest.stores.header.size() - 1;
}

function headerAt(height: number): WireBlockHeader | undefined {
	return manifest.stores.header.get(height);
}

function headerHashAt(height: number): Uint8Array | undefined {
	return manifest.stores.header.get(height)?.hash();
}

/**
 * hash -> height, but VERIFIED against the header store. A reorg leaves stale
 * blockhash rows for orphaned headers (the hashmap has no delete); this check —
 * "does the header now sitting at that height still have this hash?" — makes
 * every such stale row read back as unknown. Self-healing, no cleanup pass.
 */
function heightOfHash(hash: Uint8Array): number | undefined {
	const height = manifest.stores.headerhash.get(hash);
	if (height === undefined) return undefined;
	const at = headerHashAt(height);
	return at && equals(at, hash) ? height : undefined;
}

function buildLocators(): Uint8Array[] {
	const locators: Uint8Array[] = [];
	let step = 1;
	let index = tipHeight();
	while (index > 0) {
		locators.push(headerHashAt(index)!);
		if (locators.length >= 10) step <<= 1;
		index -= step;
	}
	const last = locators.at(-1);
	if (!last || !equals(last, GENESIS_BLOCK_HASH)) locators.push(GENESIS_BLOCK_HASH);
	return locators;
}

// ── block download engine (unchanged behaviour; localChain -> mmap helpers) ───

/** Blocks currently reserved against a peer. O(in-flight), stays ≤ window. */
function peerInFlight(peer: Peer): number {
	let n = 0;
	for (const info of blockInFlight.values()) if (info.peer === peer) n++;
	return n;
}

/**
 * Reserve + request the next blocks we don't have/aren't fetching, spreading
 * DOWNLOAD_BATCH-sized batches across live peers round-robin. Per-peer outstanding
 * is hard-capped (Core's 16) so no single send queue outruns the timeout; the
 * global window caps total in-flight. The whole want-set is reserved BEFORE any
 * await so the listener can't shrink the frontier mid-pass.
 */
async function requestBlocks(live: Peer[], fromHeight: number, top: number, globalRoom: number, rr: number): Promise<number> {
	const perPeerCap = MAX_BLOCKS_IN_TRANSIT_PER_PEER;

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
		const hash = headerHashAt(h);
		if (!hash) break;
		want.push({ height: h, hash });
	}
	if (want.length === 0) return rr;

	const now = Date.now();
	let i = 0;
	while (i < want.length) {
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
			await peer.send(GetDataMessage, { inventory: batch.map((w) => ({ type: MSG_WITNESS_BLOCK, hash: w.hash })) });
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
			// The body's txs don't hash to the header's committed merkle root, so
			// the body is invalid. Don't pool it, and blacklist the hash (which now
			// survives restarts) so header sync cuts here. Previously this threw,
			// and Peer's listener loop swallowed the throw — the block was silently
			// dropped and re-requested from the same peer forever, a no-log
			// livelock. (Distrusting the peer that served the bad body is the
			// deeper fix; deferred with the rest of the WIP peer management.)
			console.error(`[p2p] invalid merkle root for block ${encodeHex(block.header.hash())}; blacklisting`);
			blacklist.add(block.header.hash());
			return;
		}

		const height = heightOfHash(block.header.hash());
		if (height === undefined) return; // unsolicited / orphaned
		if (height <= cursor) return;
		if (blockPool.has(height)) return;

		blockInFlight.delete(height);
		lastBlockAt.set(peer, Date.now());
		blockPool.set(height, WireTxs.encode(block.txs));
	});
	blockUnlisten.set(peer, off);
}

/**
 * Guarantee the head-of-line block is (re)requested. Unlike the window — which
 * leans on per-peer liveness — the block we're packing on gets a hard per-request
 * timeout and bypasses the window cap, so a single dropped delivery can't wedge
 * the whole pack loop.
 */
function ensureHeadRequested(live: Peer[], height: number, rr: number): void {
	if (live.length === 0) return;
	const inFlight = blockInFlight.get(height);
	const now = Date.now();
	if (inFlight && now - inFlight.at <= BLOCK_TIMEOUT_MS) return;

	const hash = headerHashAt(height);
	if (!hash) return;
	const peer = live[rr % live.length]!;
	blockInFlight.set(height, { peer, at: now });
	peer.send(GetDataMessage, { inventory: [{ type: MSG_WITNESS_BLOCK, hash }] }).catch((e) => {
		console.error("[p2p] head-of-line getdata error:", e);
		blockInFlight.delete(height);
	});
}

/**
 * Release in-flight requests from peers gone silent/dropped so they retry
 * elsewhere. Liveness is per-peer: a peer steadily delivering keeps all its
 * reservations no matter how deep its queue; a truly dead one is reaped after
 * BLOCK_TIMEOUT_MS. Head-of-line recovery is ensureHeadRequested's job, not this.
 */
function reapTimedOut(): void {
	const now = Date.now();
	let reaped = 0;
	for (const [height, info] of blockInFlight) {
		if (now - info.at < BLOCK_TIMEOUT_MS) continue; // hard grace: never reap fresh reservations
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

/** Detach listeners for gone peers and release their in-flight reservations. */
function dropDisconnectedPeers(): void {
	for (const [peer, off] of blockUnlisten) {
		if (peer.connected) continue;
		off();
		blockUnlisten.delete(peer);
		lastBlockAt.delete(peer);
	}
	for (const [height, info] of blockInFlight) {
		if (!info.peer.connected) blockInFlight.delete(height);
	}
}

/**
 * Fill one chunk: download bodies in strict height order from `cursor + 1`, pack
 * their raw payloads back-to-back into a CHUNK_BYTE_BUDGET buffer, post it to the
 * chain worker. Fans getdata across peers so a slow one doesn't hold up the rest.
 */
async function syncBlocks(): Promise<void> {
	dropDisconnectedPeers();

	const chunk = new Uint8Array(CHUNK_BYTE_BUDGET);
	let chunkLen = 0;
	let packHeight = cursor + 1;
	let rr = 0;

	// Genesis (height 0) is the one body no peer will serve — Bitcoin Core
	// refuses getdata for the genesis block. Its body is a fixed constant, so
	// seed it straight into the pool as the WireTxs payload (block bytes past the
	// 80-byte header) the pack loop expects. Everything after downloads normally.
	if (packHeight === 0 && !blockPool.has(0)) {
		blockPool.set(0, GENESIS_BLOCK.subarray(WireBlockHeader.stride.size));
	}

	while (true) {
		const top = tipHeight();
		if (packHeight > top) break;

		const live = peers.values().filter((p) => p.connected).toArray();
		if (live.length === 0) break;
		for (const peer of live) ensureBlockListener(peer);

		const room = BLOCK_DOWNLOAD_WINDOW - (blockPool.size + blockInFlight.size);
		if (room > 0) rr = await requestBlocks(live, packHeight, top, room, rr);

		const now0 = Date.now();
		if (now0 - lastSyncBlocksDiag > 10 * SECOND) {
			lastSyncBlocksDiag = now0;
			console.log(
				`[p2p] syncBlocks live=${live.length} packHeight=${packHeight} top=${top} inFlight=${blockInFlight.size} pool=${blockPool.size} room=${room}`,
			);
		}

		const payload = blockPool.get(packHeight);
		if (!payload) {
			ensureHeadRequested(live, packHeight, rr); // head-of-line: must always be re-requestable
			reapTimedOut();
			// Don't stall a non-empty chunk waiting on the next body: ship what's
			// packed so the chain worker keeps making progress (and throughput
			// stays measurable). An empty chunk has nothing to ship yet — wait.
			if (chunkLen > 0) break;
			await delay(DOWNLOAD_IDLE_MS);
			continue;
		}

		if (chunkLen > 0 && chunkLen + payload.length > CHUNK_BYTE_BUDGET) break; // chunk full: ship it

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

// ── header sync (forward to chain, await its pinned ack) ──────────────────────

/**
 * Fetch headers from one peer and hand each batch to the chain worker, which
 * owns header validation, the most-work reorg rule, and the pin. We wait for its
 * "headers-applied" ack before building the next locator, because the locator is
 * read from the header mmap and must not be built until chain has pinned the tip
 * we just sent. The ack tells us how many were adopted (0 => this peer has
 * nothing new, stop) and, on a reorg below our download cursor, the height to
 * rewind block downloading to.
 */
function sendHeadersToChain(headers: WireBlockHeader[]): Promise<ApplyResult> {
	if (pendingHeaderApply) throw new Error("a header batch is already awaiting chain — syncHeaders must be serial");
	const bytes = WireBlockHeaders.encode(headers);
	const result = new Promise<ApplyResult>((resolve) => (pendingHeaderApply = resolve));
	port.postMessage({ type: "headers", data: bytes });
	return result;
}

/** Apply a rewind instruction from a chain-side reorg: pull the block download
 * cursor back to the split and drop everything we'd queued above it. */
function rewindDownloadTo(splitHeight: number): void {
	if (cursor <= splitHeight) return;
	console.warn(`[p2p] reorg: rewinding block download cursor ${cursor} -> ${splitHeight}`);
	cursor = splitHeight;
	blockPool.clear();
	blockInFlight.clear();
}

async function syncHeaders(peer: Peer): Promise<void> {
	if (!peer.connected) return;
	const lastSync = lastPeerSync.get(peer) ?? 0;
	if (Date.now() - lastSync < PEER_SYNC_COOLDOWN) return;
	lastPeerSync.set(peer, Date.now());
	console.log("[p2p] syncing headers with peer");

	while (true) {
		const responsePromise = peer.expect(HeadersMessage);
		await peer.send(GetHeadersMessage, { version: PROTOCOL_VERSION, locators: buildLocators(), stopHash: new Uint8Array(32) });
		const [{ headers }] = await responsePromise;
		if (headers.length === 0) break; // peer at tip

		// Drop any header main told us to reject before forwarding — the chain
		// worker validates links + PoW but doesn't know our blacklist. Cutting at
		// the first blacklisted header keeps the batch a contiguous branch.
		const cut = headers.findIndex((h) => blacklist.has(h.hash()));
		const batch = cut === -1 ? headers : headers.slice(0, cut);
		if (batch.length === 0) break;

		const { adopted, rewind } = await sendHeadersToChain(batch);
		if (rewind !== undefined) rewindDownloadTo(rewind);
		if (adopted === 0) break; // nothing adopted — done with this peer
	}
	console.log(`[p2p] header tip height=${tipHeight()}`);
}

async function startSyncHeaders(): Promise<void> {
	while (true) {
		for (const peer of peers) {
			try {
				await syncHeaders(peer);
			} catch (err) {
				console.error("[p2p] header sync pass failed:", err);
			}
		}
		await delay(SYNC_POLL_INTERVAL);
	}
}

// ── connection lifecycle ──────────────────────────────────────────────────────

/**
 * Keep one peer address live forever: connect, handshake, add to the working
 * set, and on any drop remove it + release its reservations and redial with
 * exponential backoff. Resolves the first time it's up so start() can proceed.
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
				backoff = RECONNECT_BASE_MS;
				console.log(`[p2p] connected ${addr.host}:${addr.port}`);
				if (!signalledUp) {
					signalledUp = true;
					resolveUp();
				}

				await new Promise<void>((resolve) => {
					const off = peer.onDisconnect(() => {
						off();
						resolve();
					});
					if (!peer.connected) {
						off();
						resolve();
					}
				});
				console.log(`[p2p] peer ${addr.host}:${addr.port} dropped, reconnecting`);
			} catch (err) {
				console.error(`[p2p] connect ${addr.host}:${addr.port} failed:`, err);
			} finally {
				peers.delete(peer);
				const off = blockUnlisten.get(peer);
				if (off) {
					off();
					blockUnlisten.delete(peer);
				}
				lastBlockAt.delete(peer);
				for (const [height, info] of blockInFlight) if (info.peer === peer) blockInFlight.delete(height);
			}

			await delay(backoff);
			backoff = Math.min(backoff * 2, RECONNECT_MAX_MS);
		}
	})();

	return up;
}

async function start(): Promise<void> {
	// Bring at least one peer up before starting the loops so the first header
	// pass has someone to talk to.
	const firstConnects = PEER_ADDRESSES.map((addr) => connectAndMaintain(addr));
	await Promise.race(firstConnects).catch(() => {});

	startSyncHeaders();
	startSyncBlocks();
	started = true;
}

// ── main-port messages ────────────────────────────────────────────────────────
async function drainMessages(): Promise<void> {
	let message;
	while ((message = messageQueue.dequeue())) {
		if (!started && message.type === "start") {
			await start();
			continue;
		}
		if (message.type === "seek") {
			cursor = message.data;
			continue;
		}
		if (message.type === "consume") {
			consumedChunks++;
			continue;
		}
		if (message.type === "headers-applied") {
			// chain finished applying + pinning the batch we forwarded. Hand the
			// result to whoever's awaiting in syncHeaders.
			const resolve = pendingHeaderApply;
			pendingHeaderApply = undefined;
			resolve?.(message.data as ApplyResult);
			continue;
		}
		if (message.type === "blacklist") {
			const [hash] = Bytes32.decode(message.data);
			blacklist.add(hash);
			continue;
		}
	}
}
