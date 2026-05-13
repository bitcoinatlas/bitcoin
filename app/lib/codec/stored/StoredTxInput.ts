import { BytesCodec, Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { COINBASE_VOUT } from "~/constants.ts";
import { OutPoint, TxInput } from "~/lib/chain/TxInput.ts";
import { SequenceLockCodec } from "~/lib/codec/SequenceLock.ts";
import { U24LE } from "~/lib/codec/primitives.ts";
import { StoredPointer } from "~/lib/codec/stored/StoredPointer.ts";
import { StoredWitness } from "~/lib/codec/stored/StoredWitness.ts";

// Use BytesCodec for scriptSig length prefix
const scriptSigCodec = new BytesCodec();

// Struct for resolved prevOut (fixed size: 6 bytes pointer + 3 bytes vout)
const resolvedPrevOutCodec = new StructCodec({
	tx: StoredPointer,
	vout: U24LE,
});

// StoredTxInput codec that decodes to TxInput runtime class
export class StoredTxInputCodec extends Codec<TxInput> {
	readonly stride = -1;

	encode(input: TxInput): Uint8Array<ArrayBuffer> {
		const data = input.data;

		let prevOutEncoded: Uint8Array;
		if (data.prevOut.txId.kind === "pointer") {
			// Resolved - use struct codec
			prevOutEncoded = new Uint8Array(1 + 6 + 3); // tag + tx + vout
			prevOutEncoded[0] = 0;
			const encoded = resolvedPrevOutCodec.encode({
				tx: data.prevOut.txId.value,
				vout: data.prevOut.vout,
			});
			prevOutEncoded.set(encoded, 1);
		} else if (data.prevOut.txId.kind === "raw") {
			const storedVout = data.prevOut.vout;
			prevOutEncoded = new Uint8Array(1 + 32 + 3); // tag + txId + vout
			prevOutEncoded[0] = 1;
			prevOutEncoded.set(data.prevOut.txId.value, 1);
			const voutBytes = U24LE.encode(storedVout);
			prevOutEncoded.set(voutBytes, 33);
		} else if (data.prevOut.txId.kind === "coinbase") {
			prevOutEncoded = new Uint8Array(1); // tag
			prevOutEncoded[0] = 2;
		} else {
			throw new Error();
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
		let txId: OutPoint["txId"];
		let vout: number;
		let prevOutBytes: number;

		if (data[0] === 0) {
			// Resolved: use StructCodec
			const [prevOut] = resolvedPrevOutCodec.decode(data.subarray(1));
			txId = { kind: "pointer", value: prevOut.tx };
			vout = prevOut.vout;
			prevOutBytes = 1 + 6 + 3;
		} else if (data[0] === 1) {
			// Unresolved: manual decode
			const rawTxId = data.subarray(1, 33);
			txId = { kind: "raw", value: rawTxId };
			[vout] = U24LE.decode(data.subarray(33));
			prevOutBytes = 1 + 32 + 3;
		} else if (data[0] === 2) {
			txId = { kind: "coinbase" };
			vout = COINBASE_VOUT;
			prevOutBytes = 1;
		} else {
			throw new Error();
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
