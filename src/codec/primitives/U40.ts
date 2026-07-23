import { Codec, Stride } from "@nomadshiba/codec";

const MAX_U40 = 2 ** 40 - 1;

export class U40Codec extends Codec<number> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 5 };

	public encoder(value: number, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(value: number, target: Uint8Array, offset: number): number;
	public encoder(value: number, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (value < 0 || value > MAX_U40 || !Number.isInteger(value)) {
			throw new RangeError(`Value out of range for U40: ${value}`);
		}
		if (target === undefined) {
			const arr = new Uint8Array(5);
			this.encoder(value, arr, 0);
			return arr;
		}
		const hi = Math.floor(value / 0x100000000);
		const lo = value >>> 0;
		target[offset!] = hi & 0xff;
		target[offset! + 1] = (lo >>> 24) & 0xff;
		target[offset! + 2] = (lo >>> 16) & 0xff;
		target[offset! + 3] = (lo >>> 8) & 0xff;
		target[offset! + 4] = lo & 0xff;
		return 5;
	}

	public decoder(data: Uint8Array, offset: number): [number, number] {
		if (data.length - offset < 5) throw new Error("Not enough bytes for U40");
		const hi = data[offset]!;
		const lo = ((data[offset + 1]! << 24) | (data[offset + 2]! << 16) | (data[offset + 3]! << 8) | data[offset + 4]!) >>> 0;
		return [hi * 0x100000000 + lo, 5];
	}
}

export const U40 = new U40Codec();
