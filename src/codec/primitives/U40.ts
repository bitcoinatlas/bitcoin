import { Codec, Stride } from "@nomadshiba/codec";

const MAX_U40 = 2 ** 40 - 1;

export class U40Codec extends Codec<number> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 5 };

	encode(value: number, target?: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
		if (value < 0 || value > MAX_U40 || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U40");
		}
		const arr = target ?? new Uint8Array(5);
		const hi = Math.floor(value / 0x100000000); // top 8 bits (0..255)
		const lo = value >>> 0; // low 32 bits
		arr[0] = hi & 0xff;
		arr[1] = (lo >>> 24) & 0xff;
		arr[2] = (lo >>> 16) & 0xff;
		arr[3] = (lo >>> 8) & 0xff;
		arr[4] = lo & 0xff;
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
