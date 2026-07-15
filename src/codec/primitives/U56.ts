import { Codec, Stride } from "@nomadshiba/codec";

export class U56Codec extends Codec<bigint> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 7 };

	public encoder(value: bigint, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(value: bigint, target: Uint8Array, offset: number): number;
	public encoder(value: bigint, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (value < 0n || value > 0x00ffffffffffffffn) {
			throw new RangeError("Value out of range for U56");
		}
		if (target === undefined) {
			const arr = new Uint8Array(7);
			this.encoder(value, arr, 0);
			return arr;
		}
		for (let i = 0; i < 7; i++) {
			target[offset! + i] = Number((value >> BigInt((6 - i) * 8)) & 0xffn);
		}
		return 7;
	}

	public decoder(data: Uint8Array, offset: number): [bigint, number] {
		if (data.length - offset < 7) throw new Error("Not enough bytes for U56");
		let value = 0n;
		for (let i = 0; i < 7; i++) {
			value |= BigInt(data[offset + i]!) << BigInt((6 - i) * 8);
		}
		return [value, 7];
	}
}

export const U56 = new U56Codec();
