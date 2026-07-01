import { sha256 } from "@noble/hashes/sha2";
import { equals } from "@std/bytes/equals";
import { atomic } from "~/chain/atomic.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { PrevOut } from "~/codec/stored/StoredTxInput.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { WireTxs } from "~/codec/wire/WireTxs.ts";
import { COINBASE_TXID, COINBASE_VOUT } from "~/constants.ts";
import { Uint8ArrayMap } from "~/libs/collections/Uint8ArrayMap.ts";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";
import { Codec, VarInt } from "@nomadshiba/codec";

const pubkey = new Uint8ArrayMap<number>();
const pubkeyHashes: Uint8Array[] = [];
const blocks: { tx: WireTx; spenders: { tx: StoredTxPointer; vin: Codec.InferOutput<typeof VarInt> }[] }[][] = [];

self.addEventListener("message", (event) => {
	const { stage, data } = event.data as { stage: string; data?: unknown };

	switch (stage) {
		case "init": {
			const buffer = data as Uint8Array;
			const pubkeys = init(buffer);
			self.postMessage(pubkeys, pubkeys.map((pubkey) => pubkey.buffer));
			break;
		}
		case "consume": {
			const pubkeyPointers = data as BigUint64Array;
			const parts = consume(pubkeyPointers);
			self.postMessage(parts, parts.map((part) => part.buffer));
			break;
		}
	}
});

function init(buffer: Uint8Array): Uint8Array[] {
	const pubkeys: Uint8Array[] = [];

	blocks.length = 0;
	pubkeyHashes.length = 0;
	pubkey.clear();

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
				let pubkeyPointer = pubkey.get(pubkeyHash);
				if (pubkeyPointer !== undefined) continue; // seen locally before already
				// not seen locally, check disk
				pubkeyPointer = atomic.stores.pubkey.get(pubkeyHash);
				if (pubkeyPointer !== undefined) {
					// seen on disk, set locally
					pubkey.set(pubkeyHash, pubkeyPointer);
				} else {
					// not seen anywhere, let master know
					pubkey.set(pubkeyHash, -1);
					pubkeys.push(output.scriptPubKey.slice());
					pubkeyHashes.push(pubkeyHash);
				}
			}
			for (const input of tx.inputs) {
				input.prevOut.txId;
			}
		}
	}

	return pubkeys;
}

function consume(pubkeyPointers: BigUint64Array): Uint8Array[] {
	const parts: Uint8Array[] = [];
	for (let i = 0; i < pubkeyPointers.length; i++) {
		const pubkeyHash = pubkeyHashes[i]!;
		const pubkeyPointer = Number(pubkeyPointers[i]!);
		pubkey.set(pubkeyHash, pubkeyPointer);
	}
	for (const txs of blocks) {
		for (const { tx } of txs) {
			const stored: StoredTx = {
				txId: tx.txId,
				locktime: tx.locktime,
				version: tx.version,
				outputs: tx.outputs.map((output) => {
					const pubkeyPointer = pubkey.get(sha256(output.scriptPubKey));
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
						const txIndex = atomic.stores.txid.get(input.prevOut.txId);
						if (!txIndex) {
							throw new Error("whaaa??");
						}
						txId = { kind: "pointer", value: txIndex.pointer };
					}
					return {
						prevOut: { txId, vout: input.prevOut.vout },
						scriptSig: input.scriptSig,
						sequence: input.sequence,
						witness: tx.witness[index] ?? [],
					};
				}),
			};
			parts.push(StoredTx.encode(stored));
		}
	}

	return parts;
}
