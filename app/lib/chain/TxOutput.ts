import { ScriptPubKey } from "~/lib/chain/utils/ScriptPubKey.ts";

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
			// In a real implementation, you would fetch the scriptPubKey from storage using the pointer value.
			// For this example, we'll just throw an error to indicate that this is not implemented.
			throw new Error("Pointer scriptPubKey not implemented");
		} else {
			return this.data.scriptPubKey;
		}
	}
}
