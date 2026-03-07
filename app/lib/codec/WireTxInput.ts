import { BytesCodec, StructCodec, U32LE } from "@nomadshiba/codec";
import { bytes32, compactSize } from "~/lib/codec/primitives.ts";
import { Codec } from "@nomadshiba/codec";

// Wire format TxInput - EXACTLY what's on the wire
// - prevOut: { txId (32 bytes), vout (4 bytes) }
// - scriptSig: CompactSize + bytes  
// - sequence: 4 bytes
// NO witness - witness is at transaction level

export type WireTxInput = Codec.Infer<typeof WireTxInput>;
export const WireTxInput = new StructCodec({
	prevOut: new StructCodec({
		txId: bytes32,
		vout: U32LE,
	}),
	scriptSig: new BytesCodec({ lengthCodec: compactSize }),
	sequence: U32LE,
});
