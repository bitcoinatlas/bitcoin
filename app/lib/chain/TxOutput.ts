import { rawScriptPubKey, ScriptPubKey } from "./ScriptPubKey.ts";
import { getTxOutputByPointer } from "~/chain.ts";

export type TxOutputData = {
	value: bigint;
	spent: boolean;
	scriptPubKey:
		| { kind: "pointer"; value: number }
		| ScriptPubKey;
};

export class TxOutput {
	public data: TxOutputData;

	constructor(data: TxOutputData) {
		this.data = data;
	}

	async getScriptPubKey(): Promise<ScriptPubKey> {
		if (this.data.scriptPubKey.kind === "pointer") {
			const output = await getTxOutputByPointer(this.data.scriptPubKey.value);
			if (output.data.scriptPubKey.kind === "pointer") {
				throw new Error([
					`scriptPubKey resolution failed: pointer ${this.data.scriptPubKey.value} points to another pointer.`,
					`Expected direct ScriptPubKey at that offset.`,
				].join(" "));
			}
			return output.data.scriptPubKey;
		} else {
			return this.data.scriptPubKey;
		}
	}

	async getRawScriptPubKey(): Promise<Uint8Array> {
		return rawScriptPubKey(await this.getScriptPubKey());
	}
}
