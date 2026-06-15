import { atomic, getTxPointerById } from "~/chain/chain.ts";
import { StoredTxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { InferBatches, InferStores } from "~/storage/Atomic.ts";
import { parseScriptPubKey, rawScriptPubKey, ScriptPubKey } from "~/chain/ScriptPubKey.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { getPrevOutTxId, PrevOut, StoredTxInput } from "~/codec/stored/StoredTxInput.ts";
import { equals } from "@std/bytes";
import { COINBASE_TXID, COINBASE_VOUT } from "~/constants.ts";
import { WireTxOutput } from "~/codec/wire/WireTxOutput.ts";
import { WireTxInput } from "~/codec/wire/WireTxInput.ts";

export const ns = {
	async getScriptPubKey(
		output: StoredTxOutput,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): Promise<ScriptPubKey> {
		if (output.scriptPubKey.kind === "pointer") {
			// TODO: Why?
			const { getTxOutputByPointer } = await import("~/chain/chain.ts");
			const resolved = await getTxOutputByPointer(output.scriptPubKey.value, batches);
			if (resolved.scriptPubKey.kind === "pointer") {
				throw new Error([
					`scriptPubKey resolution failed: pointer ${output.scriptPubKey.value} points to another pointer.`,
					`Expected direct ScriptPubKey at that offset.`,
				].join(" "));
			}
			return resolved.scriptPubKey;
		} else {
			return output.scriptPubKey;
		}
	},

	async getRawScriptPubKey(
		output: StoredTxOutput,
		batches?: InferBatches<typeof atomic, "tx"> | InferStores<typeof atomic, "tx">,
	): Promise<Uint8Array> {
		return rawScriptPubKey(await ns.getScriptPubKey(output, batches));
	},

	async toWire(storedTx: StoredTx): Promise<WireTx> {
		const { txId, version, locktime } = storedTx;

		const inputs: WireTxInput[] = [];
		const witness: Uint8Array[][] = [];

		for (const input of storedTx.inputs) {
			const prevTxId = await getPrevOutTxId(input);
			inputs.push({
				prevOut: {
					txId: prevTxId,
					vout: input.prevOut.vout,
				},
				scriptSig: input.scriptSig,
				sequence: input.sequence,
			});
			if (input.witness) witness.push(input.witness);
		}

		const outputs: WireTxOutput[] = [];
		for (const output of storedTx.outputs) {
			const scriptPubKey = await ns.getScriptPubKey(output);
			outputs.push({
				value: output.value,
				scriptPubKey: rawScriptPubKey(scriptPubKey),
			});
		}

		return { txId, version, locktime, inputs, outputs, witness };
	},

	async fromWire(wireTx: WireTx): Promise<StoredTx> {
		const inputs: StoredTxInput[] = await Promise.all(
			wireTx.inputs.map(async (wireInput, i): Promise<StoredTxInput> => {
				const prevOutTxPointer = await getTxPointerById(wireInput.prevOut.txId);
				const inputWitness = wireTx.witness[i] ?? [];

				let txId: PrevOut["txId"];
				if (prevOutTxPointer === undefined) {
					if (equals(wireInput.prevOut.txId, COINBASE_TXID) && wireInput.prevOut.vout === COINBASE_VOUT) {
						txId = { kind: "coinbase" };
					} else {
						txId = { kind: "raw", value: wireInput.prevOut.txId };
					}
				} else {
					txId = { kind: "pointer", value: prevOutTxPointer };
				}

				return {
					prevOut: { txId, vout: wireInput.prevOut.vout },
					scriptSig: wireInput.scriptSig,
					sequence: wireInput.sequence,
					witness: inputWitness,
				};
			}),
		);

		const outputs: StoredTxOutput[] = [];
		for (const wireOutput of wireTx.outputs) {
			const scriptPubKey = parseScriptPubKey(wireOutput.scriptPubKey);
			const output: StoredTxOutput = { value: wireOutput.value, scriptPubKey };
			outputs.push(output);
		}

		const tx: StoredTx = {
			txId: wireTx.txId,
			spender: 0,
			version: wireTx.version,
			locktime: wireTx.locktime,
			inputs: inputs,
			outputs: outputs,
		};

		return tx;
	},
};
