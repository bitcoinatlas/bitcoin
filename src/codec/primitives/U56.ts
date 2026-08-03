import { Codec, Stride } from "@nomadshiba/codec";

export class U56Codec extends Codec<number> {
	public readonly stride: Stride<"fixed"> = { kind: "fixed", size: 7 };

	public encoder(value: number, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(value: number, target: Uint8Array, offset: number): number;
	public encoder(value: number, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (!Number.isSafeInteger(value) || value < 0) {
			throw new RangeError("Value out of range for U56");
		}
		if (target === undefined) {
			const arr = new Uint8Array(7);
			this.encoder(value, arr, 0);
			return arr;
		}
		let remaining = value;
		for (let i = 6; i >= 0; i--) {
			const byte = remaining % 256;
			target[offset! + i] = byte;
			remaining = (remaining - byte) / 256;
		}
		return 7;
	}

	public decoder(data: Uint8Array, offset: number): [number, number] {
		if (data.length - offset < 7) throw new Error("Not enough bytes for U56");
		let value = 0;
		for (let i = 0; i < 7; i++) {
			value = value * 256 + data[offset + i]!;
		}
		if (!Number.isSafeInteger(value)) {
			throw new RangeError("Decoded value exceeds Number.MAX_SAFE_INTEGER");
		}
		return [value, 7];
	}
}

export const U56 = new U56Codec();
