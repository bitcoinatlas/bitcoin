import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { Stride } from "~/lib/codec/traits.ts";
import { decodeVarInt, encodeVarInt } from "~/lib/codec/varint.ts";

// ── Bytes ──
// Raw byte array codec. Can be fixed-size or variable-size.
// Fixed: no length prefix, size known from stride.
// Variable: varint length prefix.

export type Bytes = { stride: Stride };

export const Bytes = {
	fixed(size: number): Bytes {
		return { stride: Stride.fixed(size) };
	},
	variable(): Bytes {
		return { stride: Stride.variable() };
	},
	encode(self, value: Uint8Array) {
		if (Stride.isFixed(self.stride)) {
			if (value.length !== self.stride.value) {
				throw new RangeError(`Expected ${self.stride.value} bytes, got ${value.length}`);
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
		if (Stride.isFixed(self.stride)) {
			return [data.subarray(0, self.stride.value), self.stride.value];
		}
		const [length, prefixSize] = decodeVarInt(data);
		return [data.subarray(prefixSize, prefixSize + length), prefixSize + length];
	},
} satisfies Impl<Bytes, Codec<Bytes, Uint8Array>>;

// ── Str ──
// UTF-8 string. Varint length prefix (byte count, not char count).

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export type Str = { stride: Stride };

export const Str = {
	fixed(size: number): Str {
		return { stride: Stride.fixed(size) };
	},
	variable(): Str {
		return { stride: Stride.variable() };
	},
	encode(self, value: string) {
		const utf8 = textEncoder.encode(value);
		if (Stride.isFixed(self.stride)) {
			if (utf8.length > self.stride.value) {
				throw new RangeError(`String is ${utf8.length} bytes, exceeds max ${self.stride.value}`);
			}
			const out = new Uint8Array(self.stride.value);
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
		if (Stride.isFixed(self.stride)) {
			const buf = data.subarray(0, self.stride.value);
			let end = buf.length;
			while (end > 0 && buf[end - 1] === 0) end--;
			return [textDecoder.decode(buf.subarray(0, end)), self.stride.value];
		}
		const [length, prefixSize] = decodeVarInt(data);
		const utf8 = data.subarray(prefixSize, prefixSize + length);
		return [textDecoder.decode(utf8), prefixSize + length];
	},
} satisfies Impl<Str, Codec<Str, string>>;
