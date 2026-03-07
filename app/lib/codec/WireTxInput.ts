import { BytesCodec, Codec, StructCodec, U32LE } from "@nomadshiba/codec";
import { Bytes32, CompactSize } from "~/lib/codec/primitives.ts";
import { SequenceLock } from "./SequenceLock.ts";

// Wire format TxInput - EXACTLY what's on the wire
// - prevOut: { txId (32 bytes), vout (4 bytes) }
// - scriptSig: CompactSize + bytes
// - sequence: 4 bytes
// NO witness - witness is at transaction level

export type WireTxInput = Codec.Infer<typeof WireTxInput>;
export const WireTxInput = new StructCodec({
	prevOut: new StructCodec({
		txId: Bytes32,
		vout: U32LE,
	}),
	scriptSig: new BytesCodec({ lengthCodec: CompactSize }),
	sequence: SequenceLock,
});
