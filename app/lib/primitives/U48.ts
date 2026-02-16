import { Codec } from "@nomadshiba/codec";

const MAX_U48 = 2 ** 48 - 1;

export class U48 extends Codec<number> {
	public readonly stride = 6;
	public encode(value: number): Uint8Array {
		if (value < 0 || value > MAX_U48 || !Number.isInteger(value)) {
			throw new Error("Value out of range for U48");
		}
		const bytes = new Uint8Array(this.stride);
		// Low 4 bytes via DataView (safe with bitwise ops)
		bytes[0] = value & 0xFF;
		bytes[1] = (value >>> 8) & 0xFF;
		bytes[2] = (value >>> 16) & 0xFF;
		bytes[3] = (value >>> 24) & 0xFF;
		// High 2 bytes via Math (avoid bitwise 32-bit truncation)
		const hi = Math.floor(value / 0x100000000);
		bytes[4] = hi & 0xFF;
		bytes[5] = (hi >>> 8) & 0xFF;
		return bytes;
	}

	public decode(data: Uint8Array): [number, number] {
		if (data.length < this.stride) {
			throw new Error("Invalid data length for U48");
		}
		const lo = (data[0]! | (data[1]! << 8) | (data[2]! << 16) | (data[3]! << 24)) >>> 0;
		const hi = data[4]! | (data[5]! << 8);
		return [hi * 0x100000000 + lo, this.stride];
	}
}

export const u48 = new U48();
