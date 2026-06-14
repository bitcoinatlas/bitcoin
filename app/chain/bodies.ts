import { equals } from "@std/bytes";
import { appendTxs, localChain } from "~/chain/chain.ts";
import { Uint8ArrayMap } from "~/utils/Uint8ArrayMap.ts";
import { WireBlock } from "~/codec/wire/WireBlock.ts";
import { type PeerMessageEvent } from "~/p2p/Peer.ts";
import { BlockMessage } from "~/p2p/messages/Block.ts";
import { GetDataMessage, MSG_WITNESS_BLOCK } from "~/p2p/messages/GetData.ts";
import { peers } from "~/p2p/peers.ts";

/**
 * How many blocks ahead of the verified tip the download loop is allowed to
 * run.  Keeping this bounded limits in-flight memory while still giving the
 * download loop plenty of work to hide network round-trip latency.
 */
const DOWNLOAD_AHEAD = 100;

/**
 * How many block hashes we pack into a single getdata message.  Smaller
 * batches pipeline more naturally and avoid stalling on a single slow block.
 */
const DOWNLOAD_BATCH = 16;

/** How long to wait for any single block before giving up on it. */
const BLOCK_TIMEOUT_MS = 30_000;

// Cumulative stats for smoothed speed / ETA across ticks.
let _sessionStart = 0;
let _sessionBlocks = 0;
let _sessionBytes = 0;

type PoolEntry = {
	height: number;
	payload: Uint8Array;
	block: ReturnType<typeof WireBlock.decode>[0];
};

/**
 * Call once per tick.
 *
 * Runs two concurrent loops that share a pool map (block hash → decoded block):
 *
 *   • **Download loop** — walks allPending from the front, staying at most
 *     DOWNLOAD_AHEAD blocks ahead of the verified tip.  Emits getdata in
 *     DOWNLOAD_BATCH-sized chunks and records when each block was requested.
 *     Stops immediately when the verify loop signals it is done (via `abort`).
 *
 *   • **Verify loop** — drains the pool in strict height order, calling
 *     appendBlockTxs for each block as soon as it lands, then bumping the
 *     verified counter so the download loop can advance.  When it is done
 *     (TICK_BYTE_LIMIT reached or all blocks appended) it sets `abort = true`
 *     so the download loop does not continue firing getdata needlessly.
 */
export async function syncBodiesFromPeers(): Promise<{ height: number; timestamp: number } | null> {
	const peer = peers().find((p) => p.connected);
	if (!peer) return null;

	const invType = MSG_WITNESS_BLOCK;

	// Collect heights that are still missing a body, in order.
	// We cap at DOWNLOAD_AHEAD * a generous multiplier — the loop will re-run
	// on the next tick to continue, so there is no need to enumerate the entire
	// remaining chain upfront.
	type Pending = { height: number; hash: Uint8Array };
	const allPending: Pending[] = [];
	for (const [height, node] of localChain.entries()) {
		if (height === 0) continue; // genesis already seeded
		if (node.pointer !== null) continue;
		allPending.push({ height, hash: node.header.hash });
		// Collect enough to fill several windows worth of work per tick.
		if (allPending.length >= DOWNLOAD_AHEAD * 4) break;
	}
	if (allPending.length === 0) return null;

	if (_sessionStart === 0) _sessionStart = Date.now();

	// Shared pool populated by the message listener, drained by the verify loop.
	const pool = new Uint8ArrayMap<PoolEntry>(DOWNLOAD_AHEAD * 2);

	// verifiedCount: how many entries from allPending the verify loop has processed.
	// The download loop reads this to enforce the DOWNLOAD_AHEAD window.
	let verifiedCount = 0;

	// abort: set by the verify loop when it is done so the download loop stops.
	let abort = false;

	// requestedAt: set by the download loop when it fires getdata for a block.
	// The verify loop reads this to compute per-block deadlines.
	const requestedAt = new Uint8ArrayMap<number>(DOWNLOAD_AHEAD * 2);

	// ── Message listener ──────────────────────────────────────────────────────

	const unlistenBlocks = peer.onMessage((msg: PeerMessageEvent) => {
		if (msg.command !== BlockMessage.command) return;

		let block;
		try {
			[block] = WireBlock.decode(msg.payload);
		} catch (e) {
			console.error("[bodies] block decode error:", e);
			return;
		}

		const entry = allPending.find(({ hash }) => equals(hash, block.header.hash));
		if (!entry) return;
		if (pool.has(entry.hash)) return; // deduplicate

		pool.set(entry.hash, { height: entry.height, payload: msg.payload, block });
	});

	// ── Download loop ─────────────────────────────────────────────────────────

	const downloadLoop = (async () => {
		let nextIdx = 0;

		while (nextIdx < allPending.length && !abort && peer.connected) {
			const inFlight = nextIdx - verifiedCount;
			if (inFlight >= DOWNLOAD_AHEAD) {
				await new Promise<void>((r) => setTimeout(r, 5));
				continue;
			}

			const batchEnd = Math.min(
				nextIdx + DOWNLOAD_BATCH,
				nextIdx + (DOWNLOAD_AHEAD - inFlight),
				allPending.length,
			);
			const batch = allPending.slice(nextIdx, batchEnd);
			nextIdx = batchEnd;

			// Record request times before sending so the verify loop can
			// compute deadlines as soon as blocks are in scope.
			const now = Date.now();
			for (const { hash } of batch) requestedAt.set(hash, now);

			try {
				await peer.send(GetDataMessage, {
					inventory: batch.map(({ hash }) => ({ type: invType, hash })),
				});
			} catch (e) {
				if (!abort) console.error("[bodies] getdata send error:", e);
				break;
			}
		}
	})();

	// ── Append loop ───────────────────────────────────────────────────────────

	let appendedBytes = 0;
	let appendedCount = 0;
	let firstAppendHeight: number | undefined;
	let lastAppendHeight: number | undefined;
	let lastAppendTimestamp: number | undefined;

	const appentLoop = (async () => {
		for (const { height, hash } of allPending) {
			// Wait until the block lands in the pool.
			// Deadline is measured from when the verify loop starts actively waiting
			// for this block, not from when the batch was sent — large blocks take
			// time to process and the sent timestamp goes stale while the verify
			// loop is busy with prior blocks.
			let timedOut = false;
			let waitStart: number | undefined;
			while (!pool.has(hash)) {
				if (!peer.connected) break;
				const sentAt = requestedAt.get(hash);
				if (sentAt !== undefined) {
					if (waitStart === undefined) waitStart = Date.now();
					if (Date.now() - waitStart >= BLOCK_TIMEOUT_MS) {
						console.warn(`[bodies] timeout waiting for block height=${height}`);
						timedOut = true;
						break;
					}
				}
				await new Promise<void>((r) => setTimeout(r, 5));
			}

			// On timeout, stop processing this tick entirely — continuing would
			// cause every subsequent block (whose requestedAt timestamps are now
			// stale) to time out immediately, skipping them all.
			if (timedOut) break;

			verifiedCount++; // advance the download window

			const entry = pool.get(hash);
			if (!entry) continue;

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
				requestedAt.delete(hash);
			} catch (reason) {
				console.error(`[bodies] appendBlockTxs failed at height=${height}:`, reason);
			}
		}

		// Tell the download loop to stop — we are done for this tick.
		abort = true;

		if (appendedCount > 0) {
			_sessionBlocks += appendedCount;
			_sessionBytes += appendedBytes;
			const elapsedSec = (Date.now() - _sessionStart) / 1_000;
			const blocksPerSec = elapsedSec > 0 ? _sessionBlocks / elapsedSec : 0;
			const bytesPerSec = elapsedSec > 0 ? _sessionBytes / elapsedSec : 0;

			const remaining = [...localChain.entries()].filter(
				([h, n]) => h > 0 && n.pointer === null,
			).length;
			const etaStr = blocksPerSec > 0 && remaining > 0
				? `eta=${fmtDuration(remaining / blocksPerSec)}`
				: remaining === 0
				? "eta=done"
				: "eta=unknown";

			console.log(
				`[bodies] appended blocks=${appendedCount} heights=${firstAppendHeight}-${lastAppendHeight}` +
					` bytes=${appendedBytes} speed=${fmtBytes(bytesPerSec)}/s` +
					` blocks/s=${blocksPerSec.toFixed(1)} ${etaStr}`,
			);
		}
	})();

	try {
		await Promise.all([downloadLoop, appentLoop]);
	} finally {
		unlistenBlocks();
	}

	if (lastAppendHeight !== undefined && lastAppendTimestamp !== undefined) {
		return { height: lastAppendHeight, timestamp: lastAppendTimestamp };
	}
	return null;
}

function fmtBytes(bytes: number): string {
	if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(2)} MB`;
	if (bytes >= 1_024) return `${(bytes / 1_024).toFixed(1)} KB`;
	return `${Math.round(bytes)} B`;
}

function fmtDuration(seconds: number): string {
	const s = Math.round(seconds);
	if (s < 60) return `${s}s`;
	if (s < 3_600) return `${Math.floor(s / 60)}m${s % 60}s`;
	return `${Math.floor(s / 3_600)}h${Math.floor((s % 3_600) / 60)}m`;
}
