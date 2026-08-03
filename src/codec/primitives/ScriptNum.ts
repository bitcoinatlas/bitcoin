import { Codec, Stride } from "@nomadshiba/codec";

/**
 * `CScriptNum` — the way Bitcoin Script serializes integers on the stack.
 *
 * Little-endian, minimal length (no trailing zero bytes), sign-magnitude: the
 * high bit of the most-significant byte is the sign. So if the top byte would
 * otherwise have its high bit set, an extra `0x00` (or `0x80` for negatives) is
 * appended so the value isn't misread as the opposite sign. Zero encodes as
 * empty. This is a bare number encoding — it does NOT include the push-length
 * opcode a scriptSig would prefix it with.
 *
 * Variable-length with no self-describing terminator, so `decoder` needs the
 * caller to hand it exactly the number's bytes (e.g. via a length-prefixed
 * push); it consumes to the end of the slice it's given. Values are clamped to
 * JS safe-integer range.
 */
export class ScriptNumCodec extends Codec<number> {
	public readonly stride: Stride<"variable"> = { kind: "variable" };

	public encoder(value: number, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(value: number, target: Uint8Array, offset: number): number;
	public encoder(value: number, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		if (!Number.isSafeInteger(value)) throw new Error(`ScriptNum: unsafe integer ${value}`);

		const bytes: number[] = [];
		const negative = value < 0;
		let abs = Math.abs(value);
		while (abs > 0) {
			bytes.push(abs & 0xff);
			abs = Math.floor(abs / 256);
		}
		// Sign bit: if the top byte's high bit is set, append a byte carrying the
		// sign so the number isn't read as the opposite sign. Otherwise fold the
		// sign into the existing top byte.
		if (bytes.length > 0 && (bytes[bytes.length - 1]! & 0x80)) {
			bytes.push(negative ? 0x80 : 0x00);
		} else if (negative) {
			bytes[bytes.length - 1]! |= 0x80;
		}

		if (target === undefined) return Uint8Array.from(bytes);
		target.set(bytes, offset!);
		return bytes.length;
	}

	/**
	 * Decode the CScriptNum occupying `data[offset..end]`. Since the encoding has
	 * no terminator, this consumes the ENTIRE remaining slice — pass a subarray
	 * bounded to just the number's bytes. Returns `[value, bytesRead]`.
	 */
	public decoder(data: Uint8Array, offset: number): [number, number] {
		const len = data.length - offset;
		if (len === 0) return [0, 0];

		// Little-endian magnitude, then peel the sign bit off the top byte.
		// 2**(8*i) rather than shifts so we stay correct past 32 bits.
		let magnitude = 0;
		for (let i = 0; i < len; i++) {
			const byte = i === len - 1 ? data[offset + i]! & 0x7f : data[offset + i]!;
			magnitude += byte * 2 ** (8 * i);
		}
		const negative = (data[offset + len - 1]! & 0x80) !== 0;
		return [negative ? -magnitude : magnitude, len];
	}
}

export const ScriptNum = new ScriptNumCodec();
