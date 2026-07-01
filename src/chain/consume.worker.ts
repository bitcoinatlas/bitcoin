import { sha256 } from "@noble/hashes/sha2";
import { Codec, VarInt } from "@nomadshiba/codec";
import { equals } from "@std/bytes/equals";
import { atomic } from "~/chain/atomic.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { PrevOut } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { WireTxs } from "~/codec/wire/WireTxs.ts";
import { COINBASE_TXID, COINBASE_VOUT } from "~/constants.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { StoredPubkeyPointer } from "~/codec/stored/StoredPubkeyPointer.ts";

const pubkeyPointers = new FastUint8ArrayMap<StoredPubkeyPointer>();
const pubkeyHashes: Uint8Array[] = [];
const blocks: { tx: WireTx; spenders: { tx: StoredTxPointer; vin: Codec.InferOutput<typeof VarInt> }[] }[][] = [];
const storedTxs: StoredTxs = [];
let prevOutPointerCount = 0;

self.addEventListener("message", (event) => {
	const { stage, data } = event.data as { stage: string; data?: unknown };

	switch (stage) {
		case "init": {
			const buffer = data as Uint8Array;
			const pubkeys = init(buffer);
			const transferables: Transferable[] = [];
			for (const pubkey of pubkeys) {
				transferables.push(pubkey.buffer);
			}
			self.postMessage(pubkeys, transferables);
			break;
		}
		case "process": {
			const pubkeyPointers = data as BigUint64Array;
			const parts = process(pubkeyPointers);
			self.postMessage(parts, parts.map((part) => part.buffer));
			break;
		}
	}
});

function init(buffer: Uint8Array) {
	const pubkeys: Uint8Array[] = [];

	blocks.length = 0;
	pubkeyHashes.length = 0;
	storedTxs.length = 0;
	prevOutPointerCount = 0;
	pubkeyPointers.clear();

	let offset = 0;
	while (offset < buffer.length) {
		const [txs, size] = WireTxs.decode(buffer.subarray(offset));
		offset += size;
		blocks.push(txs.map((tx) => {
			return { tx, spenders: tx.outputs.map(() => ({ tx: 0, vin: 0 })) };
		}));
	}

	for (const txs of blocks) {
		for (const { tx } of txs) {
			for (const output of tx.outputs) {
				const pubkeyHash = sha256(output.scriptPubKey);
				let pubkeyPointer = pubkeyPointers.get(pubkeyHash);
				if (pubkeyPointer !== undefined) continue; // seen locally before already
				// not seen locally, check disk
				pubkeyPointer = atomic.stores.pubkey.get(pubkeyHash);
				if (pubkeyPointer !== undefined) {
					// seen on disk, set locally
					pubkeyPointers.put(pubkeyHash, pubkeyPointer);
				} else {
					pubkeys.push(output.scriptPubKey.slice());
					pubkeyHashes.push(pubkeyHash);
				}
			}
			for (const input of tx.inputs) {
				if (equals(input.prevOut.txId, COINBASE_TXID) && input.prevOut.vout === COINBASE_VOUT) {
					// coinbase, no prevOutTx
					continue;
				}
				prevOutPointerCount++;
			}
		}
	}

	return pubkeys;
}

function process(pubkeys: BigUint64Array): Uint8Array[] {
	const parts: Uint8Array[] = [];
	const prevOutTxPointerOffsets = new Uint8Array(prevOutPointerCount * 35); // bytes32 hash + u24 byte offset
	for (let i = 0; i < pubkeys.length; i++) {
		const pubkeyHash = pubkeyHashes[i]!;
		const pubkeyPointer = Number(pubkeys[i]!);
		pubkeyPointers.put(pubkeyHash, pubkeyPointer);
	}
	for (let blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
		const txs = blocks[blockIndex]!;
		storedTxs.length = txs.length;
		for (let txIndex = 0; txIndex < txs.length; txIndex++) {
			const { tx } = txs[txIndex]!;
			const stored: StoredTx = {
				txId: tx.txId,
				locktime: tx.locktime,
				version: tx.version,
				outputs: tx.outputs.map((output) => {
					const pubkeyPointer = pubkeyPointers.get(sha256(output.scriptPubKey));
					if (!pubkeyPointer) {
						throw new Error("pubkey pointer not found, weird");
					}
					return {
						value: output.value,
						scriptPubKey: Number(pubkeyPointer),
					};
				}),
				inputs: tx.inputs.map((input, index) => {
					let txId: PrevOut["txId"];
					if (equals(input.prevOut.txId, COINBASE_TXID) && input.prevOut.vout === COINBASE_VOUT) {
						txId = { kind: "coinbase" };
					} else {
						const txIndex = txPointers.get(input.prevOut.txId);
						if (txIndex === undefined) {
							throw new Error("whaaa??");
						}
						txId = { kind: "pointer", value: txIndex };
					}
					return {
						prevOut: { txId, vout: input.prevOut.vout },
						scriptSig: input.scriptSig,
						sequence: input.sequence,
						witness: tx.witness[index] ?? [],
					};
				}),
			};
			storedTxs[txIndex] = stored;
		}
		parts.push(StoredTxs.encode(storedTxs));
	}

	return parts;
}

function final();
