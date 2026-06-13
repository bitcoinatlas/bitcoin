import { equals } from "@std/bytes";
import { getTxPointerById } from "~/chain/chain.ts";
import { COINBASE_TXID, COINBASE_VOUT } from "~/constants.ts";
import { getPrevOutTxId, OutPoint, TxInput } from "~/chain/TxInput.ts";
import type { StoredTx } from "~/codec/stored/StoredTx.ts";
import { TxOutput } from "~/codec/stored/StoredTxOutput.ts";
import { LockTime } from "~/codec/LockTime.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";
import type { WireTxInput } from "~/codec/wire/WireTxInput.ts";
import type { WireTxOutput } from "~/codec/wire/WireTxOutput.ts";
import { parseScriptPubKey, rawScriptPubKey } from "./ScriptPubKey.ts";

export type TxData = {
	txId: Uint8Array;
	version: number;
	locktime: LockTime;
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
			const prevTxId = await getPrevOutTxId(input);
			inputs.push({
				prevOut: {
					txId: prevTxId,
					vout: input.prevOut.vout,
				},
				scriptSig: input.scriptSig,
				sequence: input.sequence,
			});
			if (this.data.witness) witness.push(input.witness);
		}

		const outputs: WireTxOutput[] = [];
		for (const output of this.data.outputs) {
			const scriptPubKey = await TxOutput.getScriptPubKey(output);
			outputs.push({
				value: output.value,
				scriptPubKey: rawScriptPubKey(scriptPubKey),
			});
		}

		return { txId, version, locktime, inputs, outputs, witness };
	}

	static async fromWire(wireTx: WireTx): Promise<Tx> {
		const inputs: TxInput[] = await Promise.all(wireTx.inputs.map(async (wireInput, i): Promise<TxInput> => {
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

			return {
				prevOut: { txId, vout: wireInput.prevOut.vout },
				scriptSig: wireInput.scriptSig,
				sequence: wireInput.sequence,
				witness: inputWitness,
			};
		}));

		const outputs: TxOutput[] = [];
		for (const wireOutput of wireTx.outputs) {
			const scriptPubKey = parseScriptPubKey(wireOutput.scriptPubKey);
			const output: TxOutput = { value: wireOutput.value, scriptPubKey };
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
			witness: storedTx.vin.some((input: TxInput) => input.witness.length > 0),
			inputs: storedTx.vin,
			outputs: storedTx.vout,
		});
	}
}
