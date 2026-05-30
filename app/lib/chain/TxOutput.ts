import { rawScriptPubKey, ScriptPubKey } from "./ScriptPubKey.ts";

export type TxOutput = {
	value: bigint;
	spent: boolean;
	scriptPubKey:
		| { kind: "pointer"; value: number }
		| ScriptPubKey;
};

export async function getScriptPubKey(output: TxOutput): Promise<ScriptPubKey> {
	if (output.scriptPubKey.kind === "pointer") {
		const { getTxOutputByPointer } = await import("~/chain.ts");
		const resolved = await getTxOutputByPointer(output.scriptPubKey.value);
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
}

export async function getRawScriptPubKey(output: TxOutput): Promise<Uint8Array> {
	return rawScriptPubKey(await getScriptPubKey(output));
}
