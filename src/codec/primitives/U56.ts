import { Codec, Stride } from "@nomadshiba/codec";

export class U56Codec extends Codec<bigint> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 7 };

	encode(value: bigint, target?: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
		if (value < 0n || value > 0x00ffffffffffffffn) {
			throw new RangeError("Value out of range for U56");
		}
		const arr = target ?? new Uint8Array(7);
		for (let i = 0; i < 7; i++) {
			arr[i] = Number((value >> BigInt((6 - i) * 8)) & 0xffn);
		}
		return arr;
	}

	decode(data: Uint8Array): [bigint, number] {
		if (data.length < 7) throw new Error("Not enough bytes for U56");
		let value = 0n;
		for (let i = 0; i < 7; i++) {
			value |= BigInt(data[i]!) << BigInt((6 - i) * 8);
		}
		return [value, 7];
	}
}

export const U56 = new U56Codec();
