import { appendBlockTxs, localChain } from "~/chain.ts";
import { MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { WireBlock } from "~/lib/codec/wire/WireBlock.ts";
import { type PeerMessageEvent } from "~/lib/peer/Peer.ts";
import { BlockMessage } from "~/lib/peer/messages/Block.ts";
import { GetDataMessage, MSG_BLOCK } from "~/lib/peer/messages/GetData.ts";
import { peers } from "~/peers.ts";

const BATCH_SIZE = 32;
const BLOCK_TIMEOUT_MS = 30_000;
const TICK_BYTE_LIMIT = MAX_BLOCK_WEIGHT * BATCH_SIZE;

/** Call once per tick. Downloads block bodies until TICK_BYTE_LIMIT bytes received or tip reached. */
export async function syncBodiesFromPeers(): Promise<void> {
	const peer = peers().find((p) => p.connected);
	if (!peer) return;

	let bytesDownloaded = 0;

	while (bytesDownloaded < TICK_BYTE_LIMIT) {
		// Collect next batch of headers missing a body.
		type Pending = { height: number; hash: Uint8Array };
		const pending: Pending[] = [];

		for (const [height, node] of localChain.entries()) {
			if (height === 0) continue; // genesis already seeded
			if (node.pointer !== null) continue;
			pending.push({ height, hash: node.header.hash });
			if (pending.length >= BATCH_SIZE) break;
		}

		if (pending.length === 0) break; // at tip

		// Send getdata for the batch.
		await peer.send(GetDataMessage, {
			inventory: pending.map(({ hash }) => ({ type: MSG_BLOCK, hash })),
		});

		// Collect responses.
		const heightByHash = new Map<string, number>(
			pending.map(({ height, hash }) => [toHex(hash), height]),
		);
		type ReceivedBlock = { height: number; payload: Uint8Array; block: ReturnType<typeof WireBlock.decode>[0] };
		const received: ReceivedBlock[] = [];

		await new Promise<void>((resolve) => {
			const tid = setTimeout(() => {
				unlisten();
				const left = heightByHash.size - received.length;
				if (left > 0) console.warn(`[bodies] timeout — ${left} block(s) not received`);
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

				const key = toHex(block.header.hash);
				const height = heightByHash.get(key);
				if (height === undefined) return; // not one we asked for

				received.push({ height, payload: msg.payload, block });
				console.log(
					`[bodies] Download height=${height} txs=${block.txs.length} bytes=${msg.payload.length} total=${bytesDownloaded}`,
				);

				if (received.length === heightByHash.size) {
					clearTimeout(tid);
					unlisten();
					resolve();
				}
			});
		});

		if (received.length === 0) break; // peer not responding

		// Process sequentially — appendBlockTxs uses a single store transaction at a time.
		for (const { height, payload, block } of received) {
			try {
				const { pointer } = await appendBlockTxs(block.txs, height);
				const node = localChain.at(height);
				if (node) node.pointer = pointer;
				bytesDownloaded += payload.length;
				console.log(
					`[bodies] Append height=${height} txs=${block.txs.length} bytes=${payload.length} total=${bytesDownloaded}`,
				);
			} catch (e) {
				console.error(`[bodies] appendBlockTxs failed at height=${height}:`, e);
			}
		}
	}
}

function toHex(bytes: Uint8Array): string {
	return Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join("");
}
