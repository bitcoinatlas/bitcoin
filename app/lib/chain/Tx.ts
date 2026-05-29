import { equals } from "@std/bytes";
import { getTxPointerById } from "~/chain.ts";
import { COINBASE_TXID, COINBASE_VOUT } from "~/constants.ts";
import { OutPoint, TxInput } from "~/lib/chain/TxInput.ts";
import { TxOutput } from "~/lib/chain/TxOutput.ts";
import type { StoredTx } from "~/lib/codec/stored/StoredTx.ts";
import { TimeLock } from "~/lib/codec/TimeLock.ts";
import { WireTx } from "~/lib/codec/wire/WireTx.ts";
import type { WireTxInput } from "~/lib/codec/wire/WireTxInput.ts";
import type { WireTxOutput } from "~/lib/codec/wire/WireTxOutput.ts";
import { parseScriptPubKey, rawScriptPubKey } from "./ScriptPubKey.ts";

export type TxData = {
	txId: Uint8Array;
	version: number;
	locktime: TimeLock;
	witness: boolean;
	inputs: TxInput[];
	outputs: TxOutput[];
};

export class Tx {
	public data: TxData;

	constructor(data: TxData) {
		this.data = data;
	}

	async toWire(): Promise<WireTx> {
		const { txId, version, locktime } = this.data;

		const inputs: WireTxInput[] = [];
		const witness: Uint8Array[][] = [];

		for (const input of this.data.inputs) {
			const txId = await input.getPrevOutTxId();
			inputs.push({
				prevOut: {
					txId,
					vout: input.data.prevOut.vout,
				},
				scriptSig: input.data.scriptSig,
				sequence: input.data.sequence,
			});
			witness.push(input.data.witness);
		}

		const outputs: WireTxOutput[] = [];
		for (const output of this.data.outputs) {
			const scriptPubKey = await output.getScriptPubKey();
			outputs.push({
				value: output.data.value,
				scriptPubKey: rawScriptPubKey(scriptPubKey),
			});
		}

		return { txId, version, locktime, inputs, outputs, witness };
	}

	static async fromWire(wireTx: WireTx): Promise<Tx> {
		const inputs: TxInput[] = [];

		await Promise.all(wireTx.inputs.map(async (wireInput, i) => {
			const prevOutTxPointer = await getTxPointerById(wireInput.prevOut.txId);
			const inputWitness = wireTx.witness[i] ?? [];

			let txId: OutPoint["txId"];
			if (prevOutTxPointer === undefined) {
				if (equals(wireInput.prevOut.txId, COINBASE_TXID) && wireInput.prevOut.vout === COINBASE_VOUT) {
					txId = { kind: "coinbase" };
				} else {
					txId = { kind: "raw", value: wireInput.prevOut.txId };
				}
			} else {
				txId = { kind: "pointer", value: prevOutTxPointer };
			}

			const input = new TxInput({
				prevOut: { txId, vout: wireInput.prevOut.vout },
				scriptSig: wireInput.scriptSig,
				sequence: wireInput.sequence,
				witness: inputWitness,
			});

			inputs.push(input);
		}));

		const outputs: TxOutput[] = [];
		for (const wireOutput of wireTx.outputs) {
			const scriptPubKey = parseScriptPubKey(wireOutput.scriptPubKey);
			const output = new TxOutput({
				value: wireOutput.value,
				spent: false,
				scriptPubKey,
			});
			outputs.push(output);
		}

		const tx = new Tx({
			txId: wireTx.txId,
			version: wireTx.version,
			locktime: wireTx.locktime,
			witness: wireTx.witness.length > 0,
			inputs,
			outputs,
		});

		return tx;
	}

	toStore(): StoredTx {
		return {
			txId: this.data.txId,
			version: this.data.version,
			lockTime: this.data.locktime,
			vout: this.data.outputs,
			vin: this.data.inputs,
		};
	}

	static fromStore(storedTx: StoredTx): Tx {
		return new Tx({
			txId: storedTx.txId,
			version: storedTx.version,
			locktime: storedTx.lockTime,
			witness: storedTx.vin.some((input: TxInput) => input.data.witness.length > 0),
			inputs: storedTx.vin,
			outputs: storedTx.vout,
		});
	}
}
