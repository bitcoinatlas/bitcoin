import { TimeLock } from "~/lib/chain/codec/TimeLock.ts";
import { WireTx } from "~/lib/chain/codec/wire/WireTx.ts";
import type { WireTxInput } from "~/lib/chain/codec/wire/WireTxInput.ts";
import type { WireTxOutput } from "~/lib/chain/codec/wire/WireTxOutput.ts";
import { TxInput } from "~/lib/chain/TxInput.ts";
import { TxOutput } from "~/lib/chain/TxOutput.ts";
import { ScriptPubKey } from "~/lib/chain/utils/ScriptPubKey.ts";
import type { StoredTx } from "~/lib/chain/codec/stored/StoredTx.ts";

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
				scriptPubKey: ScriptPubKey.toRaw(scriptPubKey),
			});
		}

		return { txId, version, locktime, inputs, outputs, witness };
	}

	static fromWire(wireTx: WireTx): Promise<Tx> {
		const inputs: TxInput[] = [];

		for (let i = 0; i < wireTx.inputs.length; i++) {
			const wireInput = wireTx.inputs[i]!;
			const inputWitness = wireTx.witness[i] ?? [];

			const input = new TxInput({
				prevOut: {
					txId: { kind: "raw", value: wireInput.prevOut.txId },
					vout: wireInput.prevOut.vout,
				},
				scriptSig: wireInput.scriptSig,
				sequence: wireInput.sequence,
				witness: inputWitness,
			});

			inputs.push(input);
		}

		const outputs: TxOutput[] = [];
		for (const wireOutput of wireTx.outputs) {
			const scriptPubKey = ScriptPubKey.fromRaw(wireOutput.scriptPubKey);
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

		return Promise.resolve(tx);
	}

	toStore(): Promise<StoredTx> {
		return Promise.resolve({
			txId: this.data.txId,
			version: this.data.version,
			lockTime: this.data.locktime,
			vout: this.data.outputs,
			vin: this.data.inputs,
		});
	}

	static fromStore(storedTx: StoredTx): Promise<Tx> {
		return Promise.resolve(
			new Tx({
				txId: storedTx.txId,
				version: storedTx.version,
				locktime: storedTx.lockTime,
				witness: storedTx.vin.some((input: TxInput) => input.data.witness.length > 0),
				inputs: storedTx.vin,
				outputs: storedTx.vout,
			}),
		);
	}
}
