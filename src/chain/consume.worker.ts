import { sha256 } from "@noble/hashes/sha2";
import { VarInt } from "@nomadshiba/codec";
import { equals } from "@std/bytes/equals";
import { StoredPubKey } from "~/codec/stored/StoredPubKey.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { WireTxs } from "~/codec/wire/WireTxs.ts";
import { COINBASE_TXID, COINBASE_VOUT, MAX_BLOCK_WEIGHT } from "~/constants.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { chainStore } from "~/chain/ChainStorage.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { FastUint8ArraySet } from "~/libs/collections/FastUint8ArraySet.ts";

/**
 * consume.worker — one parallel stage of the IBD pipeline.
 *
 * A round of N of these run at once, each owning one chunk (many blocks). The
 * heavy, embarrassingly-parallel work lives here: wire decode, scriptPubKey
 * dedup + disk lookups, prevOut tx disk lookups, and the final StoredTx encode.
 * The main thread only does what MUST be serial: assigning pointers (they depend
 * on global append order) and the ordered commit.
 *
 * Two stages, because output encoding needs pubkey pointers the main thread
 * assigns, and pointers to txs created *this batch* aren't known until commit:
 *
 *   init(chunk)              -> { pubkeys }            unknown scriptPubKeys
 *     [main thread assigns pubkey pointers across all workers, deduped]
 *   process(pubkeyPointers)  -> { blocks: EncodedBlock[] }
 *     [main thread registers txIds, patches deferred prevOuts, appends in order]
 *
 * A prevOut that resolves on disk is written straight into the encoding here (in
 * parallel — the whole point). A prevOut that misses disk is deferred: for a
 * valid chain it's funded by a tx in this same batch (another worker, or an
 * earlier block here). We write a placeholder pointer and hand the commit thread
 * a patch entry {slotOffset, prevOutTxid}. See StoredPrevOutTxId.patchPointer.
 */

/** One block, encoded and ready for the commit thread. All offsets are block-relative. */
export type EncodedBlock = {
	buffer: Uint8Array;
	txIds: Uint8Array;
	txOffsets: Uint32Array;
	patchOffsets: Uint32Array;
	patchTxids: Uint8Array;
};

export type InitResult = {
	encoded: Uint8Array;
	encodedLengths: Uint32Array;
};

/** A prevOut that missed disk, pending commit-thread resolution. */
type Deferred = { inputIndex: number; txid: Uint8Array };

const PUBKEY_PENDING = -1;

const unknownPubkeysSet = new FastUint8ArraySet();
const unknownPubkeys: Uint8Array[] = [];
const unknownPubkeyEncoded: Uint8Array[] = [];
const blocks: WireTx[][] = [];

// [LOG] prevOut resolution counters, reset each process() call.
let prevOutDiskHits = 0;
let prevOutDeferred = 0;

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
				console.log(`[${NAME}] init done ${ms(t)}ms blocks=${blocks.length} unknownPubkeys=${result.encodedLengths.length}`);
				self.postMessage(
					{ stage: "init-done", hashes: result.hashes, encoded: result.encoded, lengths: result.encodedLengths },
					[result.hashes.buffer, result.encoded.buffer, result.encodedLengths.buffer],
				);
				break;
			}
			case "process": {
				const t = performance.now();
				const encoded = process(data as BigUint64Array);
				console.log(
					`[${NAME}] process done ${
						ms(t)
					}ms blocks=${encoded.length} prevOutDiskHits=${prevOutDiskHits} prevOutDeferred=${prevOutDeferred}`,
				);
				const transfer: Transferable[] = [];
				for (const block of encoded) {
					transfer.push(
						block.buffer.buffer,
						block.txIds.buffer,
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

function init(buffer: Uint8Array): InitResult {
	blocks.length = 0;
	unknownPubkeys.length = 0;
	unknownPubkeyEncoded.length = 0;

	let offset = 0;
	while (offset < buffer.length) {
		const [txs, size] = WireTxs.decode(buffer.subarray(offset));
		offset += size;
		blocks.push(txs);
	}

	for (const txs of blocks) {
		for (const tx of txs) {
			for (const output of tx.outputs) {
				if (chainStore.stores.pubkey.has(output.scriptPubKey)) continue;
				unknownPubkeysSet.add(output.scriptPubKey);
				unknownPubkeys.push(output.scriptPubKey);
				unknownPubkeyEncoded.push(StoredPubKey.encode(output.scriptPubKey));
			}
		}
	}

	// Pack for a cheap transfer + zero re-hash/re-encode on the chain thread:
	// hashes[i] identifies pubkey i (for cross-worker dedup), encoded is every
	// pubkey's StoredPubKey bytes back-to-back, lengths[i] slices them.
	const n = unknownPubkeys.length;
	const encodedLengths = new Uint32Array(n);
	let total = 0;
	for (let i = 0; i < n; i++) {
		encodedLengths[i] = unknownPubkeyEncoded[i]!.length;
		total += unknownPubkeyEncoded[i]!.length;
	}
	const encoded = new Uint8Array(total);
	let encodedOffset = 0;
	for (let i = 0; i < n; i++) {
		encoded.set(unknownPubkeyEncoded[i]!, encodedOffset);
		encodedOffset += unknownPubkeyEncoded[i]!.length;
	}

	return { encoded, encodedLengths };
}

function process(): EncodedBlock[] {
	prevOutDiskHits = 0;
	prevOutDeferred = 0;

	const encoded: EncodedBlock[] = new Array(blocks.length);

	for (let b = 0; b < blocks.length; b++) {
		const txs = blocks[b]!;
		const txCount = txs.length;

		const txIds = new Uint8Array(txCount * 32);
		const txOffsets = new Uint32Array(txCount);
		const patchOffsets: number[] = [];
		const patchTxids: Uint8Array[] = [];

		let cursor = VarInt.encodeInto(txCount, encodeScratch, 0);

		for (let t = 0; t < txCount; t++) {
			const tx = txs[t]!;
			txIds.set(tx.txId, t * 32);

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
			txIds,
			txOffsets,
			patchOffsets: Uint32Array.from(patchOffsets),
			patchTxids: packTxids(patchTxids),
		};
	}

	return encoded;
}

function toStored(tx: WireTx): { stored: StoredTx; deferred: Deferred[] } {
	const deferred: Deferred[] = [];

	const outputs = tx.outputs.map((output): StoredTxOutput => {
		const pubKeyResult = chainStore.stores.pubkey.getValueAndPointer(output.scriptPubKey);
		if (pubKeyResult === undefined) {
			throw new Error("pubkey pointer missing after assignment");
		}
		const [pubKeyPointer, lastTxIdPointer] = pubKeyResult;
		// const [lastTxIdEntry] = chainStorage.stores.txid.getEntry(lastTxIdPointer);
		// const [lastTxId, lastTxPointer] = lastTxIdEntry;
		// const [lastTx] = chainStorage.stores.tx.get(lastTxPointer, StoredTx);
		chainStore.stores.pubkey.setValue(pubKeyPointer); // TODO: uhhhh
		return { value: Number(output.value), scriptPubKey: pubKeyPointer, previousOutputTx: lastTxIdPointer };
	});

	const inputs = tx.inputs.map((input, index): StoredTxInput => {
		const witness = tx.witness[index] ?? [];
		const base = { scriptSig: input.scriptSig, sequence: input.sequence, witness };

		if (equals(input.prevOut.txId, COINBASE_TXID) && input.prevOut.output === COINBASE_VOUT) {
			return { prevOut: { txId: { kind: "coinbase" }, output: input.prevOut.output }, ...base };
		}

		const onDiskPointer = chainStore.stores.txid.get(input.prevOut.txId);
		if (onDiskPointer !== undefined) {
			prevOutDiskHits++;
			return { prevOut: { txId: { kind: "pointer", value: onDiskPointer }, output: input.prevOut.output }, ...base };
		}

		prevOutDeferred++;
		deferred.push({ inputIndex: index, txid: input.prevOut.txId });
		return { prevOut: { txId: { kind: "pointer", value: 0 }, output: input.prevOut.output }, ...base };
	});

	return { stored: { locktime: tx.locktime, version: tx.version, outputs, inputs }, deferred };
}

function packTxids(txIds: Uint8Array[]): Uint8Array {
	const packed = new Uint8Array(txIds.length * 32);
	for (let i = 0; i < txIds.length; i++) packed.set(txIds[i]!, i * 32);
	return packed;
}
