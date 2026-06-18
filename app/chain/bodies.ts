import { appendTxs, localChain } from "~/chain/chain.ts";
import { Uint8ArrayMap } from "~/utils/Uint8ArrayMap.ts";
import { WireBlock } from "~/codec/wire/WireBlock.ts";
import { type Peer, type PeerMessageEvent } from "~/p2p/Peer.ts";
import { BlockMessage } from "~/p2p/messages/Block.ts";
import { GetDataMessage, MSG_WITNESS_BLOCK } from "~/p2p/messages/GetData.ts";
import { peers } from "~/p2p/peers.ts";
import { delay } from "@std/async";

/**
 * Soft byte budget for how much block payload we append per tick.  Once the
 * append loop has committed at least this many bytes it stops and lets the tick
 * end, then flushes.  This is the single most important throughput knob during
 * IBD: flush cost is roughly linear in staged bytes, so the budget directly
 * bounds per-flush latency.  Keep it an explicit byte count: the block-size
 * constant is the serialized max (~4 MiB), so deriving the budget from it (e.g.
 * ×30 ≈ 120 MiB) let dense-block ticks balloon to ~114 MiB and stall 5-6s in a
 * single flush, collapsing blocks/s past ~130k.  32 MiB keeps each flush in the ~1.5-2s band, firing more often, which is
 * faster on average (steady beats spiky) for the same total work — mirrors
 * Bitcoin Core's dbcache-driven flush cadence.
 */
const TICK_BYTE_BUDGET = 32 * 1024 * 1024;

/**
 * How many blocks ahead of the appended tip the downloader keeps
 * downloaded-or-in-flight at once.  Block sizes are unknown until downloaded, so
 * we keep a bounded look-ahead window to hide round-trip latency while limiting
 * in-flight memory.
 */
const DOWNLOAD_AHEAD = 500;

/**
 * How many block hashes we pack into a single getdata message.  Smaller batches
 * pipeline more naturally and avoid stalling on a single slow block.
 */
const DOWNLOAD_BATCH = 16;

/** How long to wait for any single block before giving up on it. */
const BLOCK_TIMEOUT_MS = 30_000;

/** Idle pause for the downloader when the window is full or nothing is pending. */
const DOWNLOADER_IDLE_MS = 50;

/**
 * EWMA smoothing factor for the speed estimate.  Each tick's instantaneous rate
 * is blended in as `est = alpha * sample + (1 - alpha) * est`.  Higher = more
 * responsive to recent ticks (converges fast, noisier); lower = smoother but
 * slower to react.  ~0.2 favors recent activity while still smoothing spikes.
 */
const SPEED_EWMA_ALPHA = 0.2;

// Stats for smoothed speed / ETA.  Anchored to the first tick (not module load)
// so elapsed reflects actual sync time.  Rates are an EWMA of per-tick
// instantaneous rates, so they converge quickly and weight recent ticks over
// old ones rather than being a slow cumulative average since session start.
let _sessionStart = 0; // ms timestamp of the first tick that did work
let _sessionBlocks = 0; // total appended, for the ETA remaining-count math
let _lastTickEnd = 0; // ms timestamp of the previous tick's completion
let _blocksPerSecEwma = 0; // smoothed blocks/s
let _bytesPerSecEwma = 0; // smoothed bytes/s

type PoolEntry = {
	height: number;
	payload: Uint8Array;
	block: ReturnType<typeof WireBlock.decode>[0];
};

// ── Persistent download state (shared between downloader and append ticks) ───
//
// The downloader (producer) fills `pool`; syncBodiesFromPeers (consumer) drains
// it.  `inFlight` tracks outstanding getdata so we never request a block twice.
// All three persist for the process lifetime so a downloaded body is never
// thrown away and re-requested.

/** Decoded blocks waiting to be appended, keyed by block hash. */
const pool = new Uint8ArrayMap<PoolEntry>(DOWNLOAD_AHEAD * 2);

/** Hashes for which a getdata has been sent and no body has landed yet. */
const inFlight = new Uint8ArrayMap<number>(DOWNLOAD_AHEAD * 2); // hash → requested-at ms

/** The peer we attached our persistent block listener to, and the detach fn. */
let _listenerPeer: Peer | null = null;
let _unlisten: (() => void) | null = null;

/** Whether the background downloader loop has been started. */
let _downloaderRunning = false;

// ── Frontier tracking ────────────────────────────────────────────────────────
//
// Pending bodies are always the contiguous run [_pendingTip .. localChain.height()]
// (bodies append in strict height order; genesis at height 0 already has a
// pointer). Previously every tick AND every downloader pass AND every received
// block rescanned the chain from genesis to (re)discover this frontier — an O(n)
// walk whose cost grew with height, starving both the append loop and the
// downloader as the sync advanced. Instead we keep:
//
//   _pendingTip  — lowest height not yet appended (cursor; only moves up).
//   _pendingHashes — hash → height for every pending block we've enumerated,
//                    so the block listener resolves an incoming hash in O(1)
//                    instead of scanning the chain.
//
// _pendingTip is lazily initialised on first use to the first height whose
// pointer is null (handles a partially-synced chain loaded from disk).

let _pendingTip = -1; // -1 = uninitialised
const _pendingHashes = new Uint8ArrayMap<number>(DOWNLOAD_AHEAD * 4); // hash → height

/** Initialise/repair the cursor: lowest height with pointer === null. */
function ensurePendingTip(): void {
	if (_pendingTip >= 0) return;
	const h = localChain.height();
	let tip = 1; // genesis (0) always has a pointer
	while (tip <= h && localChain.at(tip)?.pointer !== null) tip++;
	_pendingTip = tip;
}

/**
 * Reset the frontier cursor and pending-hash index. The cursor assumes the
 * chain only grows (append-only) during a session, which holds for IBD. If a
 * reorg ever rewrites history at or below the current tip (rolling a node's
 * pointer back to null), call this so the frontier is recomputed from the new
 * chain state on the next enumerate. Cheap; the next enumerate re-seeds both.
 */
export function resetPendingFrontier(): void {
	_pendingTip = -1;
	_pendingHashes.clear();
}

/**
 * Advance the cursor past any heights that are now appended (pointer set),
 * dropping their hashes from the pending index. Called after appends.
 */
function advancePendingTip(): void {
	const h = localChain.height();
	while (_pendingTip <= h) {
		const node = localChain.at(_pendingTip);
		if (!node || node.pointer === null) break;
		// This height is done; if we ever indexed its hash, drop it.
		_pendingHashes.delete(node.header.hash());
		_pendingTip++;
	}
}

const invType = MSG_WITNESS_BLOCK;

/**
 * Attach (or re-attach, if the peer changed) the persistent block listener that
 * fills `pool` regardless of which tick is running.  Idempotent for a given
 * peer.
 */
function ensureListener(peer: Peer): void {
	if (_listenerPeer === peer && _unlisten) return;

	// Peer changed (reconnect / failover) — detach the old listener and reset
	// in-flight tracking, since those requests went to a peer we no longer hold.
	if (_unlisten) {
		_unlisten();
		_unlisten = null;
		inFlight.clear();
	}

	_listenerPeer = peer;
	_unlisten = peer.onMessage((msg: PeerMessageEvent) => {
		if (msg.command !== BlockMessage.command) return;

		let block;
		try {
			[block] = WireBlock.decode(msg.payload);
		} catch (e) {
			console.error("[bodies] block decode error:", e);
			return;
		}

		const hash = block.header.hash();

		// Only keep blocks we actually requested and still need. O(1) lookup
		// against the pending-hash index instead of a full chain scan.
		const height = _pendingHashes.get(hash);
		if (height === undefined) return; // unsolicited, or already appended
		if (pool.has(hash)) return; // duplicate body

		pool.set(hash, { height, payload: msg.payload, block });
		inFlight.delete(hash);
	});
	console.log("[bodies] block listener attached");
}

type Pending = { height: number; hash: Uint8Array };

/**
 * Enumerate pending bodies in height order, starting from the frontier cursor
 * instead of genesis, capped to keep the scan cheap. Each enumerated block's
 * hash is recorded in _pendingHashes so the block listener can resolve incoming
 * bodies in O(1). The walk is bounded by `limit` and by the current chain tip,
 * so its cost is independent of how many blocks are already appended.
 */
function enumeratePending(limit: number): Pending[] {
	ensurePendingTip();
	advancePendingTip();
	const out: Pending[] = [];
	const top = localChain.height();
	for (let height = _pendingTip; height <= top; height++) {
		const node = localChain.at(height);
		if (!node || node.pointer !== null) continue;
		const hash = node.header.hash();
		_pendingHashes.set(hash, height); // index for the listener
		out.push({ height, hash });
		if (out.length >= limit) break;
	}
	return out;
}

// ── Downloader (producer) ────────────────────────────────────────────────────

/**
 * Start the background download loop.  Call once at boot.  Runs forever:
 *
 *   • Picks a connected peer and keeps the persistent listener attached to it.
 *   • Keeps the look-ahead window (pooled + in-flight) topped up to
 *     DOWNLOAD_AHEAD by firing getdata for the next pending blocks that are
 *     neither pooled nor already in-flight.
 *   • Sleeps briefly when the window is full, nothing is pending, or there is no
 *     peer — then tries again.
 *
 * It does not append anything; syncBodiesFromPeers consumes the pool.
 */
export function startDownloader(): void {
	if (_downloaderRunning) {
		console.warn("[bodies] startDownloader called twice — ignoring");
		return;
	}
	_downloaderRunning = true;
	console.log("[bodies] downloader started");
	void downloaderLoop();
}

async function downloaderLoop(): Promise<void> {
	for (;;) {
		const peer = peers().find((p) => p.connected) ?? null;
		if (!peer) {
			await delay(DOWNLOADER_IDLE_MS);
			continue;
		}
		ensureListener(peer);

		const room = DOWNLOAD_AHEAD - (pool.size + inFlight.size);

		// Wait until there's enough room for at least one full batch (or the
		// window has genuinely drained near empty / tip is close).  This stops the
		// loop from dribbling 2-block getdatas every time the listener shrinks the
		// window mid-pass.
		if (room < DOWNLOAD_BATCH) {
			await delay(DOWNLOADER_IDLE_MS);
			continue;
		}

		// Collect the next blocks we don't already have/aren't already fetching.
		const pending = enumeratePending(room * 2);
		const want: Pending[] = [];
		for (const p of pending) {
			if (want.length >= room) break;
			if (pool.has(p.hash)) continue;
			if (inFlight.has(p.hash)) continue;
			want.push(p);
		}

		// Only fire if we have a full batch worth — UNLESS the remaining pending
		// count is itself smaller than a batch (tip is near), in which case send
		// what we have so we don't stall at the end.
		const tipNear = pending.length < DOWNLOAD_BATCH;
		if (want.length === 0 || (want.length < DOWNLOAD_BATCH && !tipNear)) {
			await delay(DOWNLOADER_IDLE_MS);
			continue;
		}

		// Mark the ENTIRE want set in-flight up front, before any await.  This is
		// the key fix: if we marked per-batch, the listener (running during each
		// `await peer.send`) could move earlier blocks into the pool and the next
		// loop pass would only discover 1–2 fresh frontier blocks, collapsing
		// getdata into 2-block messages.  Reserving the whole window first keeps
		// batches full.
		const now = Date.now();
		for (const { hash } of want) inFlight.set(hash, now);

		// Fire getdata in DOWNLOAD_BATCH-sized chunks.
		for (let i = 0; i < want.length; i += DOWNLOAD_BATCH) {
			if (!peer.connected) {
				// Unsend the rest so they get re-requested once a peer is back.
				for (const { hash } of want.slice(i)) inFlight.delete(hash);
				break;
			}
			const batch = want.slice(i, i + DOWNLOAD_BATCH);
			try {
				await peer.send(GetDataMessage, {
					inventory: batch.map(({ hash }) => ({ type: invType, hash })),
				});
			} catch (e) {
				console.error("[bodies] getdata send error:", e);
				// Unsend this batch and the remainder so they retry next pass.
				for (const { hash } of want.slice(i)) inFlight.delete(hash);
				break;
			}
		}

		// Reap in-flight requests that never arrived so they can be retried.
		reapTimedOut();
	}
}

/** Drop in-flight entries older than BLOCK_TIMEOUT_MS so they get re-requested. */
function reapTimedOut(): void {
	const cutoff = Date.now() - BLOCK_TIMEOUT_MS;
	for (const [hash, requestedAt] of inFlight.entries()) {
		if (requestedAt <= cutoff) {
			console.warn("[bodies] in-flight request timed out, will retry");
			inFlight.delete(hash);
		}
	}
}

// ── Append (consumer) ────────────────────────────────────────────────────────

/**
 * Call once per tick.  Append-only: drains `pool` in strict height order,
 * calling appendTxs for each block already downloaded by the background
 * downloader, until the byte budget is hit or the next needed block isn't in
 * the pool yet.  Does NOT send any getdata — that is the downloader's job.
 *
 * Returns the new tip (height + timestamp) if anything was appended, else null.
 */
export async function syncBodiesFromPeers(): Promise<{ height: number; timestamp: number } | null> {
	if (!_downloaderRunning) {
		console.warn("[bodies] downloader not started — call startDownloader() at boot");
	}

	// The blocks we can append are the contiguous run starting at the current
	// pending tip.  Enumerate just enough to cover a byte budget of small blocks.
	const allPending = enumeratePending(DOWNLOAD_AHEAD * 8);
	if (allPending.length === 0) {
		console.log("[bodies] tick end: nothing pending");
		return null;
	}

	if (_sessionStart === 0) {
		const now = Date.now();
		_sessionStart = now;
		_lastTickEnd = now;
	}

	let appendedBytes = 0;
	let appendedCount = 0;
	let firstAppendHeight: number | undefined;
	let lastAppendHeight: number | undefined;
	let lastAppendTimestamp: number | undefined;

	for (const { height, hash } of allPending) {
		if (appendedBytes >= TICK_BYTE_BUDGET) {
			console.log(`[bodies] tick: byte budget reached (${fmtBytes(appendedBytes)})`);
			break;
		}

		// Append strictly in height order.  If the next needed block hasn't been
		// downloaded yet, stop — we don't skip ahead, and the downloader will have
		// it ready on a later tick.
		const entry = pool.get(hash);
		if (!entry) break;

		try {
			const { pointer } = await appendTxs(entry.block.txs, height);
			const node = localChain.at(height);
			if (node) node.pointer = pointer;
			appendedBytes += entry.payload.length;
			appendedCount++;
			if (firstAppendHeight === undefined) firstAppendHeight = height;
			lastAppendHeight = height;
			lastAppendTimestamp = entry.block.header.timestamp;
			pool.delete(hash);
			inFlight.delete(hash);
		} catch (reason) {
			console.error(`[bodies] appendTxs failed at height=${height}:`, reason);
			break; // don't advance past a block we couldn't append
		}
	}

	// Move the frontier past everything we just appended (drops their hashes
	// from the pending index) so the next enumerate/listener/downloader pass
	// starts at the new tip instead of rescanning from genesis.
	if (appendedCount > 0) advancePendingTip();

	if (appendedCount > 0) {
		_sessionBlocks += appendedCount;

		// Instantaneous rate for THIS tick: work done over the time actually spent
		// on it (since the previous tick ended, so the gap the tick driver waited
		// is included — this is real throughput, not just in-loop time).  Blend it
		// into the EWMA so the reported rate tracks recent activity and converges
		// fast, instead of being a slow cumulative average dragged by startup.
		const tickEnd = Date.now();
		const tickSec = Math.max(tickEnd - _lastTickEnd, 1) / 1_000;
		_lastTickEnd = tickEnd;

		const tickBlocksPerSec = appendedCount / tickSec;
		const tickBytesPerSec = appendedBytes / tickSec;

		// Seed the EWMA with the first real sample so we don't ramp up from zero.
		if (_blocksPerSecEwma === 0) {
			_blocksPerSecEwma = tickBlocksPerSec;
			_bytesPerSecEwma = tickBytesPerSec;
		} else {
			_blocksPerSecEwma += SPEED_EWMA_ALPHA * (tickBlocksPerSec - _blocksPerSecEwma);
			_bytesPerSecEwma += SPEED_EWMA_ALPHA * (tickBytesPerSec - _bytesPerSecEwma);
		}

		// Remaining pending = chain tip minus the frontier cursor. The cursor is
		// the lowest not-yet-appended height, and pending bodies are the
		// contiguous run above it, so this is exact without a chain scan.
		const remaining = Math.max(0, localChain.height() - _pendingTip + 1);
		const etaStr = _blocksPerSecEwma > 0 && remaining > 0
			? `eta=${fmtDuration(remaining / _blocksPerSecEwma)}`
			: remaining === 0
			? "eta=done"
			: "eta=unknown";

		console.log(
			`[bodies] appended blocks=${appendedCount} heights=${firstAppendHeight}-${lastAppendHeight}` +
				` bytes=${fmtBytes(appendedBytes)} speed=${fmtBytes(_bytesPerSecEwma)}/s` +
				` blocks/s=${_blocksPerSecEwma.toFixed(1)} ${etaStr}` +
				` (tick: ${fmtBytes(tickBytesPerSec)}/s ${tickBlocksPerSec.toFixed(1)} blk/s,` +
				` total=${_sessionBlocks} elapsed=${fmtDuration((tickEnd - _sessionStart) / 1_000)})`,
		);
	}

	if (lastAppendHeight !== undefined && lastAppendTimestamp !== undefined) {
		return { height: lastAppendHeight, timestamp: lastAppendTimestamp };
	}
	return null;
}

function fmtBytes(bytes: number): string {
	if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(2)} MiB`;
	if (bytes >= 1_024) return `${(bytes / 1_024).toFixed(1)} KiB`;
	return `${Math.round(bytes)} B`;
}

function fmtDuration(seconds: number): string {
	const s = Math.round(seconds);
	if (s < 60) return `${s}s`;
	if (s < 3_600) return `${Math.floor(s / 60)}m${s % 60}s`;
	return `${Math.floor(s / 3_600)}h${Math.floor((s % 3_600) / 60)}m`;
}
