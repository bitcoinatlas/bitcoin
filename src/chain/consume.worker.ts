import { sha256 } from "@noble/hashes/sha2";
import { VarInt } from "@nomadshiba/codec";
import { equals } from "@std/bytes/equals";
import { atomic } from "~/chain/atomic.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredScriptPubKey } from "~/codec/stored/StoredScriptPubkey.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { WireTxs } from "~/codec/wire/WireTxs.ts";
import { COINBASE_TXID, COINBASE_VOUT, MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";

/**
 * consume.worker — one parallel stage of the IBD pipeline.
 *
 * A round of N of these run at once, each owning one chunk (many blocks). The
 * heavy, embarrassingly-parallel work lives here: wire decode, scriptPubKey
 * dedup + disk lookups, prevout tx disk lookups, and the final StoredTx encode.
 * The main thread only does what MUST be serial: assigning pointers (they depend
 * on global append order) and the ordered commit.
 *
 * Two stages, because output encoding needs pubkey pointers the main thread
 * assigns, and pointers to txs created *this batch* aren't known until commit:
 *
 *   init(chunk)              -> { pubkeys }            unknown scriptPubKeys
 *     [main thread assigns pubkey pointers across all workers, deduped]
 *   process(pubkeyPointers)  -> { blocks: EncodedBlock[] }
 *     [main thread registers txids, patches deferred prevouts, appends in order]
 *
 * A prevout that resolves on disk is written straight into the encoding here (in
 * parallel — the whole point). A prevout that misses disk is deferred: for a
 * valid chain it's funded by a tx in this same batch (another worker, or an
 * earlier block here). We write a placeholder pointer and hand the commit thread
 * a patch entry {slotOffset, prevOutTxid}. See StoredPrevOutTxId.patchPointer.
 */

/** One block, encoded and ready for the commit thread. All offsets are block-relative. */
type EncodedBlock = {
	buffer: Uint8Array;
	txids: Uint8Array;
	txOffsets: Uint32Array;
	patchOffsets: Uint32Array;
	patchTxids: Uint8Array;
};

/** A prevout that missed disk, pending commit-thread resolution. */
type Deferred = { inputIndex: number; txid: Uint8Array };

const PUBKEY_PENDING = -1;

const pubkeyPointers = new FastUint8ArrayMap<number>();
let unknownPubkeyHashes: Uint8Array[] = [];
let blocks: WireTx[][] = [];

// [LOG] prevout resolution counters, reset each process() call.
let prevoutDiskHits = 0;
let prevoutDeferred = 0;

const encodeScratch = new Uint8Array(MAX_BLOCK_WEIGHT * 2);

const NAME = self.name || "consumer-?";
const ms = (t: number) => (performance.now() - t) | 0;

self.addEventListener("message", (event) => {
	const { stage, data } = event.data as { stage: string; data?: unknown };
	try {
		switch (stage) {
			case "init": {
				const t = performance.now();
				const result = init(data as Uint8Array);
				console.log(`[${NAME}] init done ${ms(t)}ms blocks=${blocks.length} unknownPubkeys=${result.lengths.length}`);
				self.postMessage(
					{ stage: "init-done", hashes: result.hashes, encoded: result.encoded, lengths: result.lengths },
					[result.hashes.buffer, result.encoded.buffer, result.lengths.buffer],
				);
				break;
			}
			case "process": {
				const t = performance.now();
				const encoded = process(data as BigUint64Array);
				console.log(
					`[${NAME}] process done ${ms(t)}ms blocks=${encoded.length} prevoutDiskHits=${prevoutDiskHits} deferred=${prevoutDeferred}`,
				);
				const transfer: Transferable[] = [];
				for (const block of encoded) {
					transfer.push(
						block.buffer.buffer,
						block.txids.buffer,
						block.txOffsets.buffer,
						block.patchOffsets.buffer,
						block.patchTxids.buffer,
					);
				}
				self.postMessage({ stage: "process-done", blocks: encoded }, transfer);
				break;
			}
		}
	} catch (error) {
		// Without this the main thread's await hangs forever (the done-message
		// never arrives). Report it so the commit fails loud instead of silent.
		const err = error as Error;
		console.error(`[${NAME}] ERROR in ${stage}:`, err?.stack ?? err);
		self.postMessage({ stage: "error", phase: stage, message: String(err?.message ?? err), stack: err?.stack });
	}
});

// Uncaught async / load errors too.
self.addEventListener("error", (event) => {
	console.error(`[${NAME}] uncaught error:`, (event as ErrorEvent).message);
});

// Signal the main thread that imports are done (rocksdb open) and the message
// handler above is attached, so it is safe to post work. Without this, a message
// posted during module load can be dropped and the init promise hangs forever.
console.log(`[${NAME}] ready`);
self.postMessage({ stage: "ready" });

function init(buffer: Uint8Array): { hashes: Uint8Array; encoded: Uint8Array; lengths: Uint32Array } {
	blocks = [];
	unknownPubkeyHashes = [];
	const unknownEncoded: Uint8Array[] = [];
	pubkeyPointers.clear();

	const tDecode = performance.now();
	let offset = 0;
	while (offset < buffer.length) {
		const [txs, size] = WireTxs.decode(buffer.subarray(offset));
		offset += size;
		blocks.push(txs);
	}
	const decodeMs = ms(tDecode);

	const tDedup = performance.now();
	for (const txs of blocks) {
		for (const tx of txs) {
			for (const output of tx.outputs) {
				const hash = sha256(output.scriptPubKey);
				if (pubkeyPointers.get(hash) !== undefined) continue;

				const onDisk = atomic.stores.pubkey.get(hash);
				if (onDisk !== undefined) {
					pubkeyPointers.put(hash, onDisk);
				} else {
					pubkeyPointers.put(hash, PUBKEY_PENDING);
					unknownPubkeyHashes.push(hash);
					// Encode HERE (worker), not on the chain thread — the chain thread
					// just concatenates these into one blob and does a single append.
					unknownEncoded.push(StoredScriptPubKey.encode(output.scriptPubKey));
				}
			}
		}
	}

	// Pack for a cheap transfer + zero re-hash/re-encode on the chain thread:
	// hashes[i] identifies pubkey i (for cross-worker dedup), encoded is every
	// pubkey's StoredScriptPubKey bytes back-to-back, lengths[i] slices them.
	const n = unknownPubkeyHashes.length;
	const hashes = new Uint8Array(n * 32);
	const lengths = new Uint32Array(n);
	let total = 0;
	for (let i = 0; i < n; i++) {
		hashes.set(unknownPubkeyHashes[i]!, i * 32);
		lengths[i] = unknownEncoded[i]!.length;
		total += unknownEncoded[i]!.length;
	}
	const encoded = new Uint8Array(total);
	let encOffset = 0;
	for (let i = 0; i < n; i++) {
		encoded.set(unknownEncoded[i]!, encOffset);
		encOffset += unknownEncoded[i]!.length;
	}
	console.log(`[${NAME}] init: decode=${decodeMs}ms dedup=${ms(tDedup)}ms`);

	return { hashes, encoded, lengths };
}

function process(pubkeyPointerValues: BigUint64Array): EncodedBlock[] {
	prevoutDiskHits = 0;
	prevoutDeferred = 0;

	for (let i = 0; i < unknownPubkeyHashes.length; i++) {
		pubkeyPointers.set(unknownPubkeyHashes[i]!, Number(pubkeyPointerValues[i]!));
	}

	const encoded: EncodedBlock[] = new Array(blocks.length);

	for (let b = 0; b < blocks.length; b++) {
		const txs = blocks[b]!;
		const txCount = txs.length;

		const txids = new Uint8Array(txCount * 32);
		const txOffsets = new Uint32Array(txCount);
		const patchOffsets: number[] = [];
		const patchTxids: Uint8Array[] = [];

		let cursor = VarInt.encodeInto(txCount, encodeScratch, 0);

		for (let t = 0; t < txCount; t++) {
			const tx = txs[t]!;
			txids.set(tx.txId, t * 32);

			const { stored, deferred } = toStored(tx);

			const txStart = cursor;
			txOffsets[t] = txStart;
			const offsets = StoredTx.encodeWithOffsets(stored, encodeScratch, cursor);

			for (const d of deferred) {
				patchOffsets.push(txStart + offsets.inputs[d.inputIndex]!);
				patchTxids.push(d.txid);
			}

			const lastInput = stored.inputs[stored.inputs.length - 1]!;
			const lastInputOffset = offsets.inputs[offsets.inputs.length - 1]!;
			cursor = txStart + lastInputOffset + StoredTxInput.encode(lastInput).length;
		}

		encoded[b] = {
			buffer: encodeScratch.slice(0, cursor),
			txids,
			txOffsets,
			patchOffsets: Uint32Array.from(patchOffsets),
			patchTxids: packTxids(patchTxids),
		};
	}

	return encoded;
}

function toStored(tx: WireTx): { stored: StoredTx; deferred: Deferred[] } {
	const deferred: Deferred[] = [];

	const outputs = tx.outputs.map((output) => {
		const pointer = pubkeyPointers.get(sha256(output.scriptPubKey));
		if (pointer === undefined || pointer === PUBKEY_PENDING) {
			throw new Error("pubkey pointer missing after assignment — init/process desync");
		}
		return { value: output.value, scriptPubKey: pointer };
	});

	const inputs = tx.inputs.map((input, index) => {
		const witness = tx.witness[index] ?? [];
		const base = { scriptSig: input.scriptSig, sequence: input.sequence, witness };

		if (equals(input.prevOut.txId, COINBASE_TXID) && input.prevOut.vout === COINBASE_VOUT) {
			return { prevOut: { txId: { kind: "coinbase" } as const, vout: input.prevOut.vout }, ...base };
		}

		const onDisk = atomic.stores.txid.get(input.prevOut.txId);
		if (onDisk !== undefined) {
			prevoutDiskHits++;
			return { prevOut: { txId: { kind: "pointer" as const, value: onDisk.pointer }, vout: input.prevOut.vout }, ...base };
		}

		prevoutDeferred++;
		deferred.push({ inputIndex: index, txid: input.prevOut.txId.slice() });
		return { prevOut: { txId: { kind: "pointer" as const, value: 0 }, vout: input.prevOut.vout }, ...base };
	});

	return { stored: { txId: tx.txId, locktime: tx.locktime, version: tx.version, outputs, inputs }, deferred };
}

function packTxids(txids: Uint8Array[]): Uint8Array {
	const packed = new Uint8Array(txids.length * 32);
	for (let i = 0; i < txids.length; i++) packed.set(txids[i]!, i * 32);
	return packed;
}
