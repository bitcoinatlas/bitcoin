import { BytesCodec, Codec, EnumCodec, StructCodec, U8, VarInt } from "@nomadshiba/codec";

type Output = Codec.InferOutput<typeof Pattern>;
type Input = Codec.InferInput<typeof Pattern> | Uint8Array;
const Pattern = new EnumCodec({
	raw: new StructCodec({ script: new BytesCodec({ sizer: VarInt }) }), //      	full script
	p2pkh: new StructCodec({ hash: new BytesCodec({ size: 20 }) }), //           	OP_DUP OP_HASH160 <20> OP_EQUALVERIFY OP_CHECKSIG
	p2sh: new StructCodec({ hash: new BytesCodec({ size: 20 }) }), //            	OP_HASH160 <20> OP_EQUAL
	p2wpkh: new StructCodec({ hash: new BytesCodec({ size: 20 }) }), //          	OP_0 <20>
	p2wsh: new StructCodec({ hash: new BytesCodec({ size: 32 }) }), //           	OP_0 <32>
	p2tr: new StructCodec({ key: new BytesCodec({ size: 32 }) }), //             	OP_1 <32>
	p2pk: new StructCodec({ key: new BytesCodec({ size: 33 }) }), //             	<33> OP_CHECKSIG            (compressed)
	p2pkUncompressed: new StructCodec({ key: new BytesCodec({ size: 65 }) }), // 	<65> OP_CHECKSIG     (uncompressed)
	opreturn: new StructCodec({ payload: new BytesCodec({ sizer: VarInt }) }), //	OP_RETURN <pushdata...>
}, { indexer: U8 });

export class StoredPubKeyCodec extends Codec<Output, Input> {
	public override stride = Pattern.stride;

	public override encoder(value: Input, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public override encoder(value: Input, target: Uint8Array, offset: number): number;
	public override encoder(value: Input, target: never, offset: never): number | Uint8Array<ArrayBuffer> {
		if (value instanceof Uint8Array) return Pattern.encoder(parseRawPubKey(value), target, offset);
		return Pattern.encoder(value, target, offset);
	}

	public override decoder(data: Uint8Array, offset: number): [Output, number] {
		return Pattern.decoder(data, offset);
	}

	public toRaw(pattern: Output): Uint8Array<ArrayBuffer>;
	public toRaw(pattern: Output, target: Uint8Array, offset?: number): number;
	public toRaw(pattern: Output, target?: Uint8Array, offset = 0): Uint8Array | number {
		const { kind, value } = pattern;
		switch (kind) {
			case "raw": {
				const { script } = value;
				const buf = target ?? new Uint8Array(script.length);
				buf.set(script, offset);
				return target ? script.length : buf;
			}
			case "p2pkh": {
				const buf = target ?? new Uint8Array(25);
				buf[offset] = 0x76, buf[offset + 1] = 0xa9, buf[offset + 2] = 0x14;
				buf.set(value.hash, offset + 3);
				buf[offset + 23] = 0x88, buf[offset + 24] = 0xac;
				return target ? 25 : buf;
			}
			case "p2sh": {
				const buf = target ?? new Uint8Array(23);
				buf[offset] = 0xa9, buf[offset + 1] = 0x14;
				buf.set(value.hash, offset + 2);
				buf[offset + 22] = 0x87;
				return target ? 23 : buf;
			}
			case "p2wpkh": {
				const buf = target ?? new Uint8Array(22);
				buf[offset] = 0x00, buf[offset + 1] = 0x14;
				buf.set(value.hash, offset + 2);
				return target ? 22 : buf;
			}
			case "p2wsh": {
				const buf = target ?? new Uint8Array(34);
				buf[offset] = 0x00, buf[offset + 1] = 0x20;
				buf.set(value.hash, offset + 2);
				return target ? 34 : buf;
			}
			case "p2tr": {
				const buf = target ?? new Uint8Array(34);
				buf[offset] = 0x51, buf[offset + 1] = 0x20;
				buf.set(value.key, offset + 2);
				return target ? 34 : buf;
			}
			case "p2pk": {
				const buf = target ?? new Uint8Array(35);
				buf[offset] = 0x21;
				buf.set(value.key, offset + 1);
				buf[offset + 34] = 0xac;
				return target ? 35 : buf;
			}
			case "p2pkUncompressed": {
				const buf = target ?? new Uint8Array(67);
				buf[offset] = 0x41;
				buf.set(value.key, offset + 1);
				buf[offset + 66] = 0xac;
				return target ? 67 : buf;
			}
			case "opreturn": {
				const { payload } = value;
				const buf = target ?? new Uint8Array(1 + payload.length);
				buf[offset] = 0x6a;
				buf.set(payload, offset + 1);
				return target ? 1 + payload.length : buf;
			}
		}
		throw new Error(`Unknown pubkey pattern: ${kind satisfies never}`);
	}
}

export type StoredPubKey = Codec.InferOutput<typeof StoredPubKey>;
export const StoredPubKey = new StoredPubKeyCodec();

function parseRawPubKey(script: Uint8Array): Codec.InferInput<typeof Pattern> {
	const n = script.length;
	if (n === 25 && script[0] === 0x76 && script[1] === 0xa9 && script[2] === 0x14 && script[23] === 0x88 && script[24] === 0xac) {
		return { kind: "p2pkh", value: { hash: script.subarray(3, 23) } };
	}
	if (n === 23 && script[0] === 0xa9 && script[1] === 0x14 && script[22] === 0x87) {
		return { kind: "p2sh", value: { hash: script.subarray(2, 22) } };
	}
	if (n === 22 && script[0] === 0x00 && script[1] === 0x14) {
		return { kind: "p2wpkh", value: { hash: script.subarray(2, 22) } };
	}
	if (n === 34 && script[0] === 0x00 && script[1] === 0x20) {
		return { kind: "p2wsh", value: { hash: script.subarray(2, 34) } };
	}
	if (n === 34 && script[0] === 0x51 && script[1] === 0x20) {
		return { kind: "p2tr", value: { key: script.subarray(2, 34) } };
	}
	if (n === 35 && script[0] === 0x21 && script[34] === 0xac) {
		return { kind: "p2pk", value: { key: script.subarray(1, 34) } };
	}
	if (n === 67 && script[0] === 0x41 && script[66] === 0xac) {
		return { kind: "p2pkUncompressed", value: { key: script.subarray(1, 66) } };
	}
	if (n >= 1 && script[0] === 0x6a) {
		return { kind: "opreturn", value: { payload: script.subarray(1) } };
	}
	return { kind: "raw", value: { script } };
}
