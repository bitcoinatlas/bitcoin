import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";

// ── U24 ──
// 3-byte unsigned integer, little-endian.

export type U24 = { stride: number };

export const U24 = {
	...CodecDefaults<U24>(),
	create(): U24 {
		return { stride: 3 };
	},
	encode(_self, value: number) {
		if (value < 0 || value > 0xffffff || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U24");
		}
		const arr = new Uint8Array(3);
		arr[0] = value & 0xff;
		arr[1] = (value >>> 8) & 0xff;
		arr[2] = (value >>> 16) & 0xff;
		return arr;
	},
	decode(_self, data) {
		if (data.length < 3) throw new Error("Not enough bytes for U24");
		return [data[0]! | (data[1]! << 8) | (data[2]! << 16), 3];
	},
} satisfies Impl<U24, Codec<U24, number>>;

// ── U48 ──
// 6-byte unsigned integer, little-endian. Used for StoredPointer.

const MAX_U48 = 2 ** 48 - 1;

export type U48 = { stride: number };

export const U48 = {
	...CodecDefaults<U48>(),
	create(): U48 {
		return { stride: 6 };
	},
	encode(_self, value: number) {
		if (value < 0 || value > MAX_U48 || !Number.isInteger(value)) {
			throw new RangeError("Value out of range for U48");
		}
		const arr = new Uint8Array(6);
		arr[0] = value & 0xff;
		arr[1] = (value >>> 8) & 0xff;
		arr[2] = (value >>> 16) & 0xff;
		arr[3] = (value >>> 24) & 0xff;
		const hi = Math.floor(value / 0x100000000);
		arr[4] = hi & 0xff;
		arr[5] = (hi >>> 8) & 0xff;
		return arr;
	},
	decode(_self, data) {
		if (data.length < 6) throw new Error("Not enough bytes for U48");
		const lo = (data[0]! | (data[1]! << 8) | (data[2]! << 16) | (data[3]! << 24)) >>> 0;
		const hi = data[4]! | (data[5]! << 8);
		return [hi * 0x100000000 + lo, 6];
	},
} satisfies Impl<U48, Codec<U48, number>>;

// ── U56 ──
// 7-byte unsigned bigint, little-endian. Used for StoredTxOutput header.

export type U56 = { stride: number };

export const U56 = {
	...CodecDefaults<U56>(),
	create(): U56 {
		return { stride: 7 };
	},
	encode(_self, value: bigint) {
		if (value < 0n || value > 0x00ffffffffffffffn) {
			throw new RangeError("Value out of range for U56");
		}
		const arr = new Uint8Array(7);
		for (let i = 0; i < 7; i++) {
			arr[i] = Number((value >> BigInt(i * 8)) & 0xffn);
		}
		return arr;
	},
	decode(_self, data) {
		if (data.length < 7) throw new Error("Not enough bytes for U56");
		let value = 0n;
		for (let i = 0; i < 7; i++) {
			value |= BigInt(data[i]!) << BigInt(i * 8);
		}
		return [value, 7];
	},
} satisfies Impl<U56, Codec<U56, bigint>>;

// ── Bytes32 ──
// Fixed 32-byte array. Used for block hashes, txids, merkle roots.
// Convenience wrapper around Bytes.fixed(32).

export type Bytes32 = { stride: number };

export const Bytes32 = {
	...CodecDefaults<Bytes32>(),
	create(): Bytes32 {
		return { stride: 32 };
	},
	zero(): Uint8Array {
		return new Uint8Array(32);
	},
	encode(_self, value: Uint8Array) {
		if (value.length !== 32) {
			throw new RangeError(`Expected 32 bytes, got ${value.length}`);
		}
		return value;
	},
	decode(_self, data) {
		if (data.length < 32) throw new Error("Not enough bytes for Bytes32");
		return [data.subarray(0, 32), 32];
	},
} satisfies Impl<Bytes32, Codec<Bytes32, Uint8Array>>;
