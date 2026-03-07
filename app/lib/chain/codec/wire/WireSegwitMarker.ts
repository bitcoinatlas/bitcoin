import { Codec } from "@nomadshiba/codec";

// Segwit marker codec: encodes 0x00 0x01, decodes by peeking
// This is used in the Bitcoin wire format to signal the presence of witness data
export class WireSegwitMarkerCodec extends Codec<boolean> {
	readonly stride = -1;

	encode(hasWitness: boolean): Uint8Array {
		return hasWitness ? Uint8Array.of(0x00, 0x01) : new Uint8Array(0);
	}

	decode(data: Uint8Array): [boolean, number] {
		if (data.length >= 2 && data[0] === 0x00 && data[1] === 0x01) {
			return [true, 2];
		}
		return [false, 0];
	}
}

// Uppercase singleton instance (codec convention)
export const WireSegwitMarker = new WireSegwitMarkerCodec();
