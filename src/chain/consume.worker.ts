import { equals } from "@std/bytes/equals";
import { atomic } from "~/chain/atomic.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { PrevOut, StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { WireTxs } from "~/codec/wire/WireTxs.ts";
import { COINBASE_TXID, COINBASE_VOUT } from "~/constants.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";

// Consume Blocks
const pubkey = new FastUint8ArrayMap<number>();
let pubkeysOffset = 0;
const pubkeys: Uint8Array[] = [];
const parts: Uint8Array[] = [];
self.addEventListener("message", (event) => {
	const buffer = event.data;

	parts.length = 0;
	pubkeys.length = 0;
	pubkey.clear();
	pubkeysOffset = 0;

	let offset = 0;
	while (offset < buffer.length) {
		const [txs, size] = WireTxs.decode(buffer.subarray(offset));
		offset += size;
		consume(txs);
	}

	self.postMessage(parts, parts); // done
});

function consume(txs: WireTx[]): void {
	try {
		for (const wireTx of txs) {
			const inputs: StoredTxInput[] = wireTx.inputs.map((wireInput, i): StoredTxInput => {
				let txId: PrevOut["txId"];
				if (equals(wireInput.prevOut.txId, COINBASE_TXID) && wireInput.prevOut.vout === COINBASE_VOUT) {
					txId = { kind: "coinbase" };
				} else {
					txId = { kind: "raw", value: wireInput.prevOut.txId };
				}
				return {
					prevOut: { txId, vout: wireInput.prevOut.vout },
					scriptSig: wireInput.scriptSig,
					sequence: wireInput.sequence,
					witness: wireTx.witness[i] ?? [],
				};
			});

			const outputs: StoredTxOutput[] = [];
			for (const wireOutput of wireTx.outputs) {
				// TODO: Nothing here diffrenciates between local pointer and real disk pointer.
				// Issue is since this is parallel other workers doing chunks before us might be appending pubkeys as well which offsets ours
				// UPDATE: ok so i used negative numbers for local ones? should do the job??? but the issue is we also encode here.
				// So fucked either way
				// i think one solution would some other parallel workers indexing script pubkeys before us?
				let scriptPubKey = atomic.stores.pubkey.get(wireOutput.scriptPubKey);
				if (!scriptPubKey) {
					let localPointer = pubkey.get(wireOutput.scriptPubKey);
					if (localPointer != null) {
						scriptPubKey = { pointer: -localPointer };
					} else {
						localPointer = pubkeysOffset;
						pubkeysOffset += wireOutput.scriptPubKey.length;
						pubkeys.push(wireOutput.scriptPubKey);
						pubkey.set(wireOutput.scriptPubKey, localPointer);
						scriptPubKey = { pointer: -localPointer };
					}
				}
				const output: StoredTxOutput = { value: Number(wireOutput.value), scriptPubKey: scriptPubKey.pointer };
				outputs.push(output);
			}

			const stored: StoredTx = {
				txId: wireTx.txId,
				version: wireTx.version,
				locktime: wireTx.locktime,
				inputs: inputs,
				outputs: outputs,
			};

			parts.push(StoredTx.encode(stored));
		}
		// parse and encode the txs and block, push the parts
		// basic validation
		// in memory writes, fallback reads to the disk
	} catch (reason) {
		console.error("Failed to append txs:", reason);
		Deno.exit(1);
	}
}
