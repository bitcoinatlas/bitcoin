import { Codec, Stride } from "@nomadshiba/codec";

const MAX_U40 = 2 ** 40 - 1;

export class U40Codec extends Codec<number> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 5 };

	encode(value: number): Uint8Array<ArrayBuffer> {
		if (value < 0 || value > MAX_U40 || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U40");
		}
		const arr = new Uint8Array(5);
		arr[0] = (value >>> 32) & 0xff;
		arr[1] = (value >>> 24) & 0xff;
		arr[2] = (value >>> 16) & 0xff;
		arr[3] = (value >>> 8) & 0xff;
		arr[4] = value & 0xff;
		return arr;
	}

	decode(data: Uint8Array): [number, number] {
		if (data.length < 5) throw new Error("Not enough bytes for U40");
		const hi = data[0]!;
		const lo = ((data[1]! << 24) | (data[2]! << 16) | (data[3]! << 8) | data[4]!) >>> 0;
		return [hi * 0x100000000 + lo, 5];
	}
}

export const U40 = new U40Codec();
