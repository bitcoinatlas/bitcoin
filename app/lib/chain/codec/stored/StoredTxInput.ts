import { BytesCodec, Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { TxInput } from "~/lib/chain/TxInput.ts";
import { SequenceLockCodec } from "~/lib/chain/codec/SequenceLock.ts";
import { U24LE } from "~/lib/codec/primitives.ts";
import { StoredPointer } from "~/lib/chain/codec/stored/StoredPointer.ts";
import { StoredWitness } from "~/lib/chain/codec/stored/StoredWitness.ts";

// Use BytesCodec for scriptSig length prefix
const scriptSigCodec = new BytesCodec();

// Struct for resolved prevOut (fixed size: 6 bytes pointer + 3 bytes vout)
const resolvedPrevOutCodec = new StructCodec({
	tx: StoredPointer,
	vout: U24LE,
});

// Unresolved uses raw txId (32 bytes) + vout (3 bytes)
const UNRESOLVED_SIZE = 32 + 3;

// StoredTxInput codec that decodes to TxInput runtime class
export class StoredTxInputCodec extends Codec<TxInput> {
	readonly stride = -1;

	encode(input: TxInput): Uint8Array {
		const data = input.data;

		let prevOutEncoded: Uint8Array;
		if (data.prevOut.txId.kind === "pointer") {
			// Resolved - use struct codec
			prevOutEncoded = new Uint8Array(1 + 6 + 3); // tag + tx + vout
			prevOutEncoded[0] = 0; // resolved tag
			const encoded = resolvedPrevOutCodec.encode({
				tx: data.prevOut.txId.value,
				vout: data.prevOut.vout,
			});
			prevOutEncoded.set(encoded, 1);
		} else {
			// Unresolved - raw txId + vout
			prevOutEncoded = new Uint8Array(1 + 32 + 3); // tag + txId + vout
			prevOutEncoded[0] = 1; // unresolved tag
			prevOutEncoded.set(data.prevOut.txId.value, 1);
			const voutBytes = U24LE.encode(data.prevOut.vout);
			prevOutEncoded.set(voutBytes, 33);
		}

		const sequenceEncoded = U32LE.encode(SequenceLockCodec.toU32(data.sequence));
		const scriptSigEncoded = scriptSigCodec.encode(data.scriptSig);
		const witnessEncoded = StoredWitness.encode(data.witness);

		const totalLength = prevOutEncoded.length + sequenceEncoded.length + scriptSigEncoded.length +
			witnessEncoded.length;
		const result = new Uint8Array(totalLength);
		let offset = 0;

		result.set(prevOutEncoded, offset);
		offset += prevOutEncoded.length;
		result.set(sequenceEncoded, offset);
		offset += sequenceEncoded.length;
		result.set(scriptSigEncoded, offset);
		offset += scriptSigEncoded.length;
		result.set(witnessEncoded, offset);

		return result;
	}

	decode(data: Uint8Array): [TxInput, number] {
		let offset = 0;

		// First byte is tag: 0 = resolved, 1 = unresolved
		const isResolved = data[0] === 0;
		let txId: { kind: "pointer"; value: number } | { kind: "raw"; value: Uint8Array };
		let vout: number;
		let prevOutBytes: number;

		if (isResolved) {
			// Resolved: use StructCodec
			const [prevOut] = resolvedPrevOutCodec.decode(data.subarray(1));
			txId = { kind: "pointer", value: prevOut.tx };
			vout = prevOut.vout;
			prevOutBytes = 1 + 6 + 3;
		} else {
			// Unresolved: manual decode
			txId = { kind: "raw", value: data.subarray(1, 33) };
			[vout] = U24LE.decode(data.subarray(33));
			prevOutBytes = 1 + 32 + 3;
		}
		offset += prevOutBytes;

		const [sequence] = U32LE.decode(data.subarray(offset));
		offset += 4;

		const [scriptSig, scriptSigBytes] = scriptSigCodec.decode(data.subarray(offset));
		offset += scriptSigBytes;

		const [witness, witnessBytes] = StoredWitness.decode(data.subarray(offset));
		offset += witnessBytes;

		const input = new TxInput({
			prevOut: { txId, vout },
			scriptSig,
			sequence: SequenceLockCodec.fromU32(sequence),
			witness,
		});

		return [input, offset];
	}
}

export const StoredTxInput = new StoredTxInputCodec();
