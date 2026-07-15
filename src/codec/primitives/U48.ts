import { Codec, Stride } from "@nomadshiba/codec";

const MAX_U48 = 2 ** 48 - 1;

export class U48Codec extends Codec<number> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 6 };

	public encoder(value: number, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(value: number, target: Uint8Array, offset: number): number;
	public encoder(value: number, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (value < 0 || value > MAX_U48 || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U48");
		}
		if (target === undefined) {
			const arr = new Uint8Array(6);
			this.encoder(value, arr, 0);
			return arr;
		}
		const hi = Math.floor(value / 0x100000000);
		target[offset!] = (hi >>> 8) & 0xff;
		target[offset! + 1] = hi & 0xff;
		target[offset! + 2] = (value >>> 24) & 0xff;
		target[offset! + 3] = (value >>> 16) & 0xff;
		target[offset! + 4] = (value >>> 8) & 0xff;
		target[offset! + 5] = value & 0xff;
		return 6;
	}

	public decoder(data: Uint8Array, offset: number): [number, number] {
		if (data.length - offset < 6) throw new Error("Not enough bytes for U48");
		const hi = (data[offset]! << 8) | data[offset + 1]!;
		const lo = ((data[offset + 2]! << 24) | (data[offset + 3]! << 16) | (data[offset + 4]! << 8) | data[offset + 5]!) >>> 0;
		return [hi * 0x100000000 + lo, 6];
	}
}

export const U48 = new U48Codec();
