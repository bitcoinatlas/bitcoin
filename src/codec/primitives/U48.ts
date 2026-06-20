import { Codec, Stride } from "@nomadshiba/codec";

const MAX_U48 = 2 ** 48 - 1;

export class U48Codec extends Codec<number> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 6 };

	public encode(value: number): Uint8Array<ArrayBuffer> {
		const arr = new Uint8Array(6);
		this.encodeInto(value, arr);
		return arr;
	}

	public override encodeInto(value: number, target: Uint8Array, offset: number = 0): number {
		if (value < 0 || value > MAX_U48 || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U48");
		}
		const hi = Math.floor(value / 0x100000000);
		target[offset] = (hi >>> 8) & 0xff;
		target[offset + 1] = hi & 0xff;
		target[offset + 2] = (value >>> 24) & 0xff;
		target[offset + 3] = (value >>> 16) & 0xff;
		target[offset + 4] = (value >>> 8) & 0xff;
		target[offset + 5] = value & 0xff;
		return 6;
	}

	public decode(data: Uint8Array): [number, number] {
		if (data.length < 6) throw new Error("Not enough bytes for U48");
		const hi = (data[0]! << 8) | data[1]!;
		const lo = ((data[2]! << 24) | (data[3]! << 16) | (data[4]! << 8) | data[5]!) >>> 0;
		return [hi * 0x100000000 + lo, 6];
	}
}

export const U48 = new U48Codec();
