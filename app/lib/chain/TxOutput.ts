import { ScriptPubKey, rawScriptPubKey } from "./ScriptPubKey.ts";

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
			throw new Error("Pointer scriptPubKey not implemented");
		} else {
			return this.data.scriptPubKey;
		}
	}

	async getRawScriptPubKey(): Promise<Uint8Array> {
		return rawScriptPubKey(await this.getScriptPubKey());
	}
}
