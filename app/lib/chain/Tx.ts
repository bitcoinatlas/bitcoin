import { TimeLock } from "../codec/TimeLock.ts";
import { WireTx } from "../codec/WireTx.ts";
import type { WireTxInput } from "../codec/WireTxInput.ts";
import type { WireTxOutput } from "../codec/WireTxOutput.ts";
import { TxInput } from "./TxInput.ts";
import { TxOutput } from "./TxOutput.ts";
import { ScriptPubKey } from "./utils/ScriptPubKey.ts";

export type TxData = {
	version: number;
	locktime: TimeLock;
	witness: boolean;
	inputs: TxInput[];
	output: TxOutput[];
};

export class Tx {
	public data: TxData;

	constructor(data: TxData) {
		this.data = data;
	}

	async toWireTx(): Promise<WireTx> {
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
		for (const output of this.data.output) {
			const scriptPubKey = await output.getScriptPubKey();
			outputs.push({
				value: output.data.value,
				scriptPubKey: ScriptPubKey.toRaw(scriptPubKey),
			});
		}

		return {
			version: this.data.version,
			locktime: this.data.locktime,
			inputs,
			output: outputs,
			witness: this.data.witness ? witness : [],
		};
	}

	static fromWireTx(wireTx: WireTx): Promise<Tx> {
		const inputs: TxInput[] = [];

		for (let i = 0; i < wireTx.inputs.length; i++) {
			const wireInput = wireTx.inputs[i]!;
			const inputWitness = wireTx.witness[i] ?? [];

			inputs.push(
				new TxInput({
					prevOut: {
						txId: { kind: "raw", value: wireInput.prevOut.txId },
						vout: wireInput.prevOut.vout,
					},
					scriptSig: wireInput.scriptSig,
					sequence: wireInput.sequence,
					witness: inputWitness,
				}),
			);
		}

		const outputs: TxOutput[] = [];
		for (const wireOutput of wireTx.output) {
			const scriptPubKey = ScriptPubKey.fromRaw(wireOutput.scriptPubKey);
			outputs.push(
				new TxOutput({
					value: wireOutput.value,
					spent: false,
					scriptPubKey,
				}),
			);
		}

		return Promise.resolve(
			new Tx({
				version: wireTx.version,
				locktime: wireTx.locktime,
				witness: wireTx.witness.length > 0,
				inputs,
				output: outputs,
			}),
		);
	}
}
