import { Codec, Stride } from "@nomadshiba/codec";

export class U24Codec extends Codec<number> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 3 };

	public encode(value: number): Uint8Array<ArrayBuffer> {
		if (value < 0 || value > 0xffffff || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U24");
		}
		const arr = new Uint8Array(3);
		arr[0] = (value >>> 16) & 0xff;
		arr[1] = (value >>> 8) & 0xff;
		arr[2] = value & 0xff;
		return arr;
	}

	public override encodeInto(value: number, target: Uint8Array, offset: number = 0): number {
		if (value < 0 || value > 0xffffff || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U24");
		}
		target[offset] = (value >>> 16) & 0xff;
		target[offset + 1] = (value >>> 8) & 0xff;
		target[offset + 2] = value & 0xff;
		return 3;
	}

	public decodeFrom(data: Uint8Array, offset: number): [number, number] {
		if (data.length - offset < 3) throw new Error("Not enough bytes for U24");
		const value = (data[offset]! << 16) | (data[offset + 1]! << 8) | data[offset + 2]!;
		return [value, 3];
	}
}

export const U24 = new U24Codec();
