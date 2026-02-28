// ── VarInt (unsigned LEB128) ──

export function encodeVarInt(value: number): Uint8Array {
	if (value < 0 || !Number.isSafeInteger(value)) {
		throw new RangeError("Value must be a non-negative safe integer");
	}
	const parts: number[] = [];
	while (value > 0x7f) {
		parts.push((value & 0x7f) | 0x80);
		value >>>= 7;
	}
	parts.push(value & 0x7f);
	return new Uint8Array(parts);
}

export function decodeVarInt(data: Uint8Array): [number, number] {
	let value = 0;
	let shift = 0;
	let bytesRead = 0;
	for (const byte of data) {
		value |= (byte & 0x7f) << shift;
		bytesRead++;
		if ((byte & 0x80) === 0) {
			if (!Number.isSafeInteger(value)) {
				throw new RangeError("Decoded value exceeds MAX_SAFE_INTEGER");
			}
			return [value, bytesRead];
		}
		shift += 7;
		if (shift > 53) {
			throw new RangeError("VarInt too long for JS safe integer");
		}
	}
	throw new Error("Incomplete VarInt");
}
