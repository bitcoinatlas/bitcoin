import { Codec, Stride } from "@nomadshiba/codec";

// Segwit marker codec: encodes 0x00 0x01, decodes by peeking
// This is used in the Bitcoin wire format to signal the presence of witness data
export class WireSegwitMarkerCodec extends Codec<boolean> {
	readonly stride: Stride<"variable"> = { kind: "variable" };

	public encoder(hasWitness: boolean, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(hasWitness: boolean, target: Uint8Array, offset: number): number;
	public encoder(hasWitness: boolean, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (target === undefined) return hasWitness ? Uint8Array.of(0x00, 0x01) : new Uint8Array(0);
		if (!hasWitness) return 0;
		offset = offset!;
		target[offset] = 0x00;
		target[offset + 1] = 0x01;
		return 2;
	}

	public decoder(data: Uint8Array, offset: number): [boolean, number] {
		if (data.length - offset >= 2 && data[offset] === 0x00 && data[offset + 1] === 0x01) {
			return [true, 2];
		}
		return [false, 0];
	}
}

// Uppercase singleton instance (codec convention)
export const WireSegwitMarker = new WireSegwitMarkerCodec();
