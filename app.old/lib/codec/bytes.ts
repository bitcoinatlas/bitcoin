import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { decodeVarInt, encodeVarInt } from "~/lib/codec/varint.ts";
import { Stride } from "./mod.ts";

// ── Bytes ──
// Raw byte array codec. Can be fixed-size or variable-size.
// Fixed (stride >= 0): no length prefix, size known from stride.
// Variable (stride < 0): varint length prefix.

export type Bytes = { stride: Stride };

export const Bytes = {
	fixed(size: number) {
		return { stride: Stride.fixed(size) };
	},
	variable() {
		return { stride: Stride.variable() };
	},
	stride(self) {
		return self.stride;
	},
	encode(self, value) {
		if (self.stride.type === "fixed") {
			if (value.length !== self.stride.size) {
				throw new RangeError(`Expected ${self.stride.size} bytes, got ${value.length}`);
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
		if (self.stride.type === "fixed") {
			return [data.subarray(0, self.stride.size), self.stride.size];
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
	fixed(size: number) {
		return { stride: Stride.fixed(size) };
	},
	variable() {
		return { stride: Stride.variable() };
	},
	stride(self) {
		return self.stride;
	},
	encode(self, value) {
		const utf8 = textEncoder.encode(value);
		if (self.stride.type === "fixed") {
			if (utf8.length > self.stride.size) {
				throw new RangeError(`String is ${utf8.length} bytes, exceeds max ${self.stride.size}`);
			}
			const out = new Uint8Array(self.stride.size);
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
		if (self.stride.type === "fixed") {
			const buf = data.subarray(0, self.stride.size);
			let end = buf.length;
			while (end > 0 && buf[end - 1] === 0) end--;
			return [textDecoder.decode(buf.subarray(0, end)), self.stride.size];
		}
		const [length, prefixSize] = decodeVarInt(data);
		const utf8 = data.subarray(prefixSize, prefixSize + length);
		return [textDecoder.decode(utf8), prefixSize + length];
	},
} satisfies Impl<Str, Codec<Str, string>>;
