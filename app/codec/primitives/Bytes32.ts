import { Codec, Stride } from "@nomadshiba/codec";

export class Bytes32Codec extends Codec<Uint8Array> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 32 };

	encode(value: Uint8Array): Uint8Array<ArrayBuffer> {
		if (value.length !== 32) {
			throw new RangeError(`Expected 32 bytes, got ${value.length}`);
		}
		return value.slice();
	}

	decode(data: Uint8Array): [Uint8Array, number] {
		if (data.length < 32) throw new Error("Not enough bytes for Bytes32");
		return [data.subarray(0, 32), 32];
	}
}

export const Bytes32 = new Bytes32Codec();
