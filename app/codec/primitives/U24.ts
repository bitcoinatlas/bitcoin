import { Codec, Stride } from "@nomadshiba/codec";

export class U24Codec extends Codec<number> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 3 };

	encode(value: number, target?: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
		if (value < 0 || value > 0xffffff || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U24");
		}
		const arr = target ?? new Uint8Array(3);
		arr[0] = (value >>> 16) & 0xff;
		arr[1] = (value >>> 8) & 0xff;
		arr[2] = value & 0xff;
		return arr;
	}

	decode(data: Uint8Array): [number, number] {
		if (data.length < 3) throw new Error("Not enough bytes for U24");
		const value = (data[0]! << 16) | (data[1]! << 8) | data[2]!;
		return [value, 3];
	}
}

export const U24 = new U24Codec();
