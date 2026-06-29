import { Codec, Stride } from "@nomadshiba/codec";

export class U56Codec extends Codec<bigint> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 7 };

	public encode(value: bigint): Uint8Array<ArrayBuffer> {
		const arr = new Uint8Array(7);
		this.encodeInto(value, arr);
		return arr;
	}

	public override encodeInto(value: bigint, target: Uint8Array, offset: number = 0): number {
		if (value < 0n || value > 0x00ffffffffffffffn) {
			throw new RangeError("Value out of range for U56");
		}
		for (let i = 0; i < 7; i++) {
			target[offset + i] = Number((value >> BigInt((6 - i) * 8)) & 0xffn);
		}
		return 7;
	}

	public decodeFrom(data: Uint8Array, offset: number): [bigint, number] {
		if (data.length - offset < 7) throw new Error("Not enough bytes for U56");
		let value = 0n;
		for (let i = 0; i < 7; i++) {
			value |= BigInt(data[offset + i]!) << BigInt((6 - i) * 8);
		}
		return [value, 7];
	}
}

export const U56 = new U56Codec();
