import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { decodeVarInt, encodeVarInt } from "~/lib/codec/varint.ts";

// ── Bytes ──
// Raw byte array codec. Can be fixed-size or variable-size.
// Fixed (stride >= 0): no length prefix, size known from stride.
// Variable (stride < 0): varint length prefix.

export type Bytes = { stride: number };

export const Bytes = {
	...CodecDefaults<Bytes>(),
	fixed(size: number): Bytes {
		return { stride: size };
	},
	variable(): Bytes {
		return { stride: -1 };
	},
	encode(self, value: Uint8Array) {
		if (self.stride >= 0) {
			if (value.length !== self.stride) {
				throw new RangeError(`Expected ${self.stride} bytes, got ${value.length}`);
			}
			return value;
		}
		const prefix = encodeVarInt(value.length);
		const out = new Uint8Array(prefix.length + value.length);
		out.set(prefix, 0);
		out.set(value, prefix.length);
		return out;
	},
	decode(self, data) {
		if (self.stride >= 0) {
			return [data.subarray(0, self.stride), self.stride];
		}
		const [length, prefixSize] = decodeVarInt(data);
		return [data.subarray(prefixSize, prefixSize + length), prefixSize + length];
	},
} satisfies Impl<Bytes, Codec<Bytes, Uint8Array>>;

// ── Str ──
// UTF-8 string. Varint length prefix (byte count, not char count).

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export type Str = { stride: number };

export const Str = {
	...CodecDefaults<Str>(),
	fixed(size: number): Str {
		return { stride: size };
	},
	variable(): Str {
		return { stride: -1 };
	},
	encode(self, value: string) {
		const utf8 = textEncoder.encode(value);
		if (self.stride >= 0) {
			if (utf8.length > self.stride) {
				throw new RangeError(`String is ${utf8.length} bytes, exceeds max ${self.stride}`);
			}
			const out = new Uint8Array(self.stride);
			out.set(utf8, 0);
			return out;
		}
		const prefix = encodeVarInt(utf8.length);
		const out = new Uint8Array(prefix.length + utf8.length);
		out.set(prefix, 0);
		out.set(utf8, prefix.length);
		return out;
	},
	decode(self, data) {
		if (self.stride >= 0) {
			const buf = data.subarray(0, self.stride);
			let end = buf.length;
			while (end > 0 && buf[end - 1] === 0) end--;
			return [textDecoder.decode(buf.subarray(0, end)), self.stride];
		}
		const [length, prefixSize] = decodeVarInt(data);
		const utf8 = data.subarray(prefixSize, prefixSize + length);
		return [textDecoder.decode(utf8), prefixSize + length];
	},
} satisfies Impl<Str, Codec<Str, string>>;
