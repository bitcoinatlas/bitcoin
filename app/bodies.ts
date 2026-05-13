import { equals } from "@std/bytes";
import { appendBlockTxs, localChain } from "~/chain.ts";
import { MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { Uint8ArrayMap } from "~/lib/Uint8ArrayMap.ts";
import { WireBlock } from "~/lib/codec/wire/WireBlock.ts";
import { type PeerMessageEvent } from "~/lib/peer/Peer.ts";
import { BlockMessage } from "~/lib/peer/messages/Block.ts";
import { GetDataMessage, MSG_BLOCK } from "~/lib/peer/messages/GetData.ts";
import { peers } from "~/peers.ts";

const BATCH_SIZE = 32;
const BLOCK_TIMEOUT_MS = 30_000;
const TICK_BYTE_LIMIT = MAX_BLOCK_WEIGHT * BATCH_SIZE;

type PoolBlock = { height: number; payload: Uint8Array; block: ReturnType<typeof WireBlock.decode>[0] };

/** Call once per tick. Downloads block bodies until TICK_BYTE_LIMIT bytes received or tip reached. */
export async function syncBodiesFromPeers(): Promise<void> {
	const peer = peers().find((p) => p.connected);
	if (!peer) return;

	// Collect all headers missing a body for this tick.
	type Pending = { height: number; hash: Uint8Array };
	const pending: Pending[] = [];
	for (const [height, node] of localChain.entries()) {
		if (height === 0) continue; // genesis already seeded
		if (node.pointer !== null) continue;
		pending.push({ height, hash: node.header.hash });
		if (pending.length >= BATCH_SIZE) break;
	}
	if (pending.length === 0) return;

	const pool = new Uint8ArrayMap<PoolBlock>(pending.length);
	let downloadedBytes = 0;

	// Download: send getdata and fill the pool as blocks arrive.
	const downloadDone = new Promise<void>((resolve) => {
		const remaining = new Set(pending.map(({ height }) => height));

		const tid = setTimeout(() => {
			unlisten();
			if (remaining.size > 0) console.warn(`[bodies] timeout — ${remaining.size} block(s) not received`);
			resolve();
		}, BLOCK_TIMEOUT_MS);

		const unlisten = peer.onMessage((msg: PeerMessageEvent) => {
			if (msg.command !== BlockMessage.command) return;

			let block;
			try {
				[block] = WireBlock.decode(msg.payload);
			} catch (e) {
				console.error("[bodies] block decode error:", e);
				return;
			}

			const entry = pending.find(({ hash }) => equals(hash, block.header.hash));
			if (!entry) return;

			pool.set(block.header.hash, { height: entry.height, payload: msg.payload, block });
			downloadedBytes += msg.payload.length;

			remaining.delete(entry.height);
			if (remaining.size === 0) {
				clearTimeout(tid);
				unlisten();
				resolve();
			}
		});

		peer.send(GetDataMessage, {
			inventory: pending.map(({ hash }) => ({ type: MSG_BLOCK, hash })),
		});
	});

	// Append: drain pool in height order as blocks become available.
	let appendedBytes = 0;
	let appendedCount = 0;
	let firstAppendHeight: number | undefined;
	let lastAppendHeight: number | undefined;
	const appendDone = (async () => {
		for (const { height, hash } of pending) {
			// Wait until this block is in the pool or download finishes.
			while (!pool.has(hash)) {
				if (downloadedBytes >= TICK_BYTE_LIMIT) break;
				await new Promise((r) => setTimeout(r, 10));
			}

			const entry = pool.get(hash);
			if (!entry) continue; // timed out or not received

			try {
				const { pointer } = await appendBlockTxs(entry.block.txs, height);
				const node = localChain.at(height);
				if (node) node.pointer = pointer;
				appendedBytes += entry.payload.length;
				appendedCount++;
				if (firstAppendHeight === undefined) firstAppendHeight = height;
				lastAppendHeight = height;
				pool.delete(hash);
			} catch (e) {
				console.error(`[bodies] appendBlockTxs failed at height=${height}:`, e);
			}

			if (appendedBytes >= TICK_BYTE_LIMIT) break;
		}
		if (appendedCount > 0) {
			console.log(
				`[bodies] appended blocks=${appendedCount} heights=${firstAppendHeight}-${lastAppendHeight} bytes=${appendedBytes}`,
			);
		}
	})();

	await Promise.all([downloadDone, appendDone]);
}

