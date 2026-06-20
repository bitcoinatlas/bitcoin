import { Codec, Stride } from "@nomadshiba/codec";

const MAX_U40 = 2 ** 40 - 1;

export class U40Codec extends Codec<number> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 5 };

	public encode(value: number): Uint8Array<ArrayBuffer> {
		const arr = new Uint8Array(5);
		this.encodeInto(value, arr);
		return arr;
	}

	public override encodeInto(value: number, target: Uint8Array, offset: number = 0): number {
		if (value < 0 || value > MAX_U40 || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U40");
		}
		const hi = Math.floor(value / 0x100000000);
		const lo = value >>> 0;
		target[offset] = hi & 0xff;
		target[offset + 1] = (lo >>> 24) & 0xff;
		target[offset + 2] = (lo >>> 16) & 0xff;
		target[offset + 3] = (lo >>> 8) & 0xff;
		target[offset + 4] = lo & 0xff;
		return 5;
	}

	public decode(data: Uint8Array): [number, number] {
		if (data.length < 5) throw new Error("Not enough bytes for U40");
		const hi = data[0]!;
		const lo = ((data[1]! << 24) | (data[2]! << 16) | (data[3]! << 8) | data[4]!) >>> 0;
		return [hi * 0x100000000 + lo, 5];
	}
}

export const U40 = new U40Codec();
