import { sha256 } from "@noble/hashes/sha2";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { rawScriptPubKey } from "~/chain/ScriptPubKey.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { MAX_BLOCK_SIZE, MAX_NON_WITNESS_BLOCK_SIZE } from "~/constants.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { formatHash } from "~/app/frontend/utils/format.ts";

// Consume Txs
const parts: Uint8Array[] = [];
self.addEventListener("message", (event) => {
	const buffer = event.data;
	parts.length = 0;
	console.log(`[chain] new chunk to consume size=${buffer.length}`);
	let offset = 0;
	while (offset < buffer.length) {
		const [txs, size] = StoredTxs.decode(buffer.subarray(offset));
		offset += size;
		parts.push(consume(txs));
	}

	self.postMessage(parts, parts); // done
});

const rawScriptPubKeyBuffer = new Uint8Array(new ArrayBuffer(0, { maxByteLength: MAX_NON_WITNESS_BLOCK_SIZE }));
// input prevOut txids (skip raw-coinbase; same-block ones resolve locally in phase 2)
const txidKeys = new FastUint8ArrayMap<number>(64);
// output scriptPubKey hashes
const pubkeyKeys = new FastUint8ArrayMap<number>(64);
const pubkeyHashes: (Uint8Array | null)[] = []; // [t][i] -> hash or null, computed once
const txScratch = new Uint8Array(MAX_BLOCK_SIZE);
function consume(txs: StoredTxs): Uint8Array {
	try {
		const txCountBytes = StoredTxs.counter.encode(txs.length);
		const blockPointer = atomic.stores.tx.append(txCountBytes);

		// --- PHASE 1: prefetch all RocksDB reads up front, in parallel ---
		// We don't know same-block pointers yet, so we just prefetch *whether* each key
		// exists in RocksDB. Same-block cases are handled by local maps in phase 2.

		txidKeys.clear();
		pubkeyKeys.clear();
		pubkeyHashes.length = 0;
		for (let t = 0; t < txs.length; t++) {
			const tx = txs[t]!;
			for (const input of tx.inputs) {
				if (input.prevOut.txId.kind === "raw") txidKeys.set(input.prevOut.txId.value, 1);
			}
			for (const output of tx.outputs) {
				if (output.scriptPubKey.kind === "pointer") {
					pubkeyHashes.push(null);
					continue;
				}
				// computed ONCE here, reused everywhere below
				const hash = sha256(rawScriptPubKey(output.scriptPubKey, rawScriptPubKeyBuffer));
				pubkeyHashes.push(hash);
				pubkeyKeys.set(hash, 1);
			}
		}

		const txidPrefetch = new FastUint8ArrayMap<StoredPointer>(txidKeys.size());
		const pubkeyPrefetch = new FastUint8ArrayMap<StoredPointer>(pubkeyKeys.size());
		for (const id of txidKeys.keys()) {
			const p = atomic.stores.txid.get(id);
			if (p !== undefined) txidPrefetch.set(id, p);
		}
		for (const h of pubkeyKeys.keys()) {
			const p = atomic.stores.pubkey.get(h);
			if (p !== undefined) pubkeyPrefetch.set(h, p);
		}

		// --- PHASE 2: sequential, no RocksDB ---
		const blockTxIds = new FastUint8ArrayMap<number>(txs.length * 2); // same-block txid -> pointer
		const blockPubkeys = new FastUint8ArrayMap<StoredPointer>(64); // same-block hash -> pointer
		let pubKeyHashesOffset = 0;
		let offset = txCountBytes.length;
		for (let t = 0; t < txs.length; t++) {
			const tx = txs[t]!;
			const txPointer = blockPointer + offset;
			tx.spender = batch.spender.length();
			batch.txid.set(tx.txId, txPointer);
			blockTxIds.set(tx.txId, txPointer);

			// inputs: local block map first, then prefetched
			for (let i = 0; i < tx.inputs.length; i++) {
				const input = tx.inputs[i]!;
				if (input.prevOut.txId.kind !== "raw") continue;
				const id = input.prevOut.txId.value;
				const pointer = blockTxIds.get(id) ?? txidPrefetch.get(id);
				if (pointer === undefined) {
					console.error(`[appendTxs] unresolved prevOut height=${height} tx=${t} vin=${i}`);
					Deno.exit(1);
				}
				input.prevOut.txId = { kind: "pointer", value: pointer };
			}

			// scriptPubKey reuse: local block map first, then prefetched
			for (let i = 0; i < tx.outputs.length; i++) {
				const output = tx.outputs[i]!;
				if (output.scriptPubKey.kind === "pointer") continue;
				const hash = pubkeyHashes[pubKeyHashesOffset + i]!;
				const existing = blockPubkeys.get(hash) ?? pubkeyPrefetch.get(hash);
				if (existing !== undefined) output.scriptPubKey = { kind: "pointer", value: existing };
			}

			// size is computed AFTER resolution above — raw→pointer changes the byte length
			const size = StoredTx.size(tx);
			const offsets = StoredTx.encodeWithOffsets(tx, txScratch, 0);
			const written = batch.tx.append(txScratch.subarray(0, size));
			if (written !== txPointer) {
				throw new Error(`[appendTxs] pointer drift: append=${written} txPointer=${txPointer}`);
			}

			// pubkey index writes: reuse the same hashes, dedup via local + prefetch
			for (let i = 0; i < tx.outputs.length; i++) {
				const output = tx.outputs[i]!;
				batch.spender.push(0);
				if (output.scriptPubKey.kind === "pointer") continue;
				const hash = pubkeyHashes[pubKeyHashesOffset + i]!;
				if (blockPubkeys.get(hash) === undefined && pubkeyPrefetch.get(hash) === undefined) {
					const ptr = txPointer + offsets.outputs[i]!;
					batch.pubkey.set(hash, ptr);
					blockPubkeys.set(hash, ptr); // so a later same-block output reuses it
				}
			}

			for (let i = 0; i < tx.inputs.length; i++) {
				const input = tx.inputs[i]!;
				if (input.prevOut.txId.kind !== "pointer") continue;
				const txSpenderOffset = batch.tx.get(input.prevOut.txId.value + Bytes32.stride.size, U40);
				const spenderIndex = txSpenderOffset + input.prevOut.vout;
				const spender = batch.spender.get(spenderIndex);
				if (spender && spender > 0) {
					const txid = batch.tx.get(input.prevOut.txId.value, Bytes32);
					throw new Error(`Output ${formatHash(txid)}:${input.prevOut.vout} is already spent.`);
				}
				batch.spender.set(spenderIndex, txPointer);
			}

			offset += size;
			pubKeyHashesOffset = tx.outputs.length;
		}

		const currentLength = batch.block.length();
		if (currentLength !== height) throw new Error(`Unexpected length=${height}, got ${currentLength}`);
		batch.block.push(blockPointer);

		return { pointer: blockPointer };
	} catch (reason) {
		console.error("Failed to append txs:", reason);
		Deno.exit(1);
	}
}
