import { BytesCodec, VarInt } from "@nomadshiba/codec";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { chainStorage } from "~/chain/ChainStorage.ts";

/**
 * spender.worker — indexes who spends each output, in parallel, off the IBD
 * commit path.
 *
 * The spender index is `(fundingTxPointer, output) -> spenderTxPointer`. Every
 * non-coinbase input names a prevOut; the funding tx already resolved to an
 * absolute blob pointer at commit time (it's stored right in the input), and the
 * spending tx's own pointer is `blockBase + txOffset` — the SAME pointer the
 * txid index stores — so the entry can be built purely by reading committed data
 * back and walking it. No live pipeline state, no coordination with the commit
 * thread.
 *
 * Why it's safe to run detached (NOTES.md §3):
 *   - It only ever reads data the writer has PINNED (refresh() bounds every read
 *     to rollback.size), so it can't see a half-written round.
 *   - It needs no write transaction. Each input claims its output exactly once,
 *     so a plain put is the norm; the orchestrator's contiguous checkpoint +
 *     idempotent replay covers restarts. See setNoOverwrite for the double-spend
 *     check that replaces batching.
 *
 * One message in, one message out:
 *   { type: "index", from, to }  ->  { type: "index-done", from, to, entries }
 * `from`/`to` are a half-open height range [from, to). A double spend (two txs
 * claiming one output) or any decode failure comes back as { type: "error" }.
 */

const NAME = self.name || "spender-?";
const ms = (t: number) => (performance.now() - t) | 0;

const { blocks, txs, spenders } = chainStorage.stores;

self.addEventListener("message", (event) => {
	const { type, from, to } = event.data as { type: string; from: number; to: number };
	if (type !== "index") return;
	try {
		const t = performance.now();
		const entries = index(from, to);
		console.log(`[${NAME}] indexed heights ${from}..${to} entries=${entries} ${ms(t)}ms`);
		self.postMessage({ type: "index-done", from, to, entries });
	} catch (error) {
		const err = error as Error;
		console.error(`[${NAME}] ERROR indexing ${from}..${to}:`, err?.stack ?? err);
		self.postMessage({ type: "error", from, to, message: String(err?.message ?? err), stack: err?.stack });
	}
});

self.addEventListener("error", (event) => {
	console.error(`[${NAME}] uncaught error:`, (event as ErrorEvent).message);
});

console.log(`[${NAME}] ready`);
self.postMessage({ type: "ready" });

/**
 * Index every spend in heights [from, to). Reveals data pinned since the worker
 * last ran, then walks each block's txs in order. Returns the number of spender
 * entries written+replayed (coinbase inputs excluded — they fund nothing).
 */
function index(from: number, to: number): number {
	// Pull in everything committed since we last looked. Both stores are pinned
	// together each round, so their revealed sizes stay consistent.
	blocks.refresh();
	txs.refresh();

	const committedBlocks = blocks.length();
	if (to > committedBlocks) {
		throw new Error(`asked to index up to ${to} but only ${committedBlocks} blocks are committed`);
	}

	const txsPinnedSize = txs.size();
	let entries = 0;

	for (let height = from; height < to; height++) {
		// Block h's txs occupy [base, end) in the txs blob. Height 0 lives at
		// offset 0 (matches ChainStore's height===0 shortcut); the last committed
		// block runs to the pinned tail.
		const base = height === 0 ? 0 : blocks.get(height)!;
		const end = height + 1 < committedBlocks ? blocks.get(height + 1)! : txsPinnedSize;
		const bytes = txs.get(base, new BytesCodec({ size: end - base }));

		let cursor = 0;
		const [txCount, countSize] = VarInt.decode(bytes, cursor);
		cursor += countSize;

		for (let t = 0; t < txCount; t++) {
			const spenderPointer = base + cursor; // == the pointer the txid index holds
			const [tx, size] = StoredTx.decode(bytes, cursor);
			cursor += size;

			for (const input of tx.inputs) {
				const prevOut = input.prevOut;
				// Coinbase inputs spend nothing; skip them.
				if (prevOut.txId.kind !== "pointer") continue;

				const key = { tx: prevOut.txId.value, output: prevOut.output };
				const guard = spenders.setNoOverwrite(key, spenderPointer);
				if (guard === "conflict") {
					const existing = spenders.get(key);
					throw new Error(
						`double spend at height=${height}: output (tx=${prevOut.txId.value}, vout=${prevOut.output}) ` +
							`already spent by tx pointer ${existing}, now also claimed by ${spenderPointer}`,
					);
				}
				entries++;
			}
		}
	}

	return entries;
}
