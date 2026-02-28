import type { Codec } from "~/lib/codec/traits.ts";
import type { Impl } from "~/traits.ts";
import { Stride } from "./mod.ts";

// ── U8 ──

export type U8 = { stride: Stride };

export const U8 = {
	create(): U8 {
		return { stride: Stride.fixed(1) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: number) {
		return new Uint8Array([value & 0xff]);
	},
	decode(_self, data) {
		return [data[0]!, 1];
	},
} satisfies Impl<U8, Codec<U8, number>>;

// ── I8 ──

export type I8 = { stride: Stride };

export const I8 = {
	create(): I8 {
		return { stride: Stride.fixed(1) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: number) {
		const arr = new Uint8Array(1);
		new DataView(arr.buffer).setInt8(0, value);
		return arr;
	},
	decode(_self, data) {
		const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
		return [view.getInt8(0), 1];
	},
} satisfies Impl<I8, Codec<I8, number>>;

// ── U16 ──

export type U16 = { stride: Stride };

export const U16 = {
	create() {
		return { stride: Stride.fixed(2) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: number) {
		const arr = new Uint8Array(2);
		new DataView(arr.buffer).setUint16(0, value, true);
		return arr;
	},
	decode(_self, data) {
		const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
		return [view.getUint16(0, true), 2];
	},
} satisfies Impl<U16, Codec<U16, number>>;

// ── I16 ──

export type I16 = { stride: Stride };

export const I16 = {
	create(): I16 {
		return { stride: Stride.fixed(2) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: number) {
		const arr = new Uint8Array(2);
		new DataView(arr.buffer).setInt16(0, value, true);
		return arr;
	},
	decode(_self, data) {
		const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
		return [view.getInt16(0, true), 2];
	},
} satisfies Impl<I16, Codec<I16, number>>;

// ── U32 ──

export type U32 = { stride: Stride };

export const U32 = {
	create(): U32 {
		return { stride: Stride.fixed(4) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: number) {
		const arr = new Uint8Array(4);
		new DataView(arr.buffer).setUint32(0, value, true);
		return arr;
	},
	decode(_self, data) {
		const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
		return [view.getUint32(0, true), 4];
	},
} satisfies Impl<U32, Codec<U32, number>>;

// ── I32 ──

export type I32 = { stride: Stride };

export const I32 = {
	create(): I32 {
		return { stride: Stride.fixed(4) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: number) {
		const arr = new Uint8Array(4);
		new DataView(arr.buffer).setInt32(0, value, true);
		return arr;
	},
	decode(_self, data) {
		const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
		return [view.getInt32(0, true), 4];
	},
} satisfies Impl<I32, Codec<I32, number>>;

// ── U64 ──

export type U64 = { stride: Stride };

export const U64 = {
	create(): U64 {
		return { stride: Stride.fixed(8) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: bigint) {
		const arr = new Uint8Array(8);
		new DataView(arr.buffer).setBigUint64(0, value, true);
		return arr;
	},
	decode(_self, data) {
		const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
		return [view.getBigUint64(0, true), 8];
	},
} satisfies Impl<U64, Codec<U64, bigint>>;

// ── I64 ──

export type I64 = { stride: Stride };

export const I64 = {
	create(): I64 {
		return { stride: Stride.fixed(8) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: bigint) {
		const arr = new Uint8Array(8);
		new DataView(arr.buffer).setBigInt64(0, value, true);
		return arr;
	},
	decode(_self, data) {
		const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
		return [view.getBigInt64(0, true), 8];
	},
} satisfies Impl<I64, Codec<I64, bigint>>;

// ── Bool ──

export type Bool = { stride: Stride };

export const Bool = {
	create(): Bool {
		return { stride: Stride.fixed(1) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: boolean) {
		return new Uint8Array([value ? 1 : 0]);
	},
	decode(_self, data) {
		return [data[0] !== 0, 1];
	},
} satisfies Impl<Bool, Codec<Bool, boolean>>;

// ── F32 ──

export type F32 = { stride: Stride };

export const F32 = {
	create(): F32 {
		return { stride: Stride.fixed(4) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: number) {
		const arr = new Uint8Array(4);
		new DataView(arr.buffer).setFloat32(0, value, true);
		return arr;
	},
	decode(_self, data) {
		const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
		return [view.getFloat32(0, true), 4];
	},
} satisfies Impl<F32, Codec<F32, number>>;

// ── F64 ──

export type F64 = { stride: Stride };

export const F64 = {
	create(): F64 {
		return { stride: Stride.fixed(8) };
	},
	stride(self) {
		return self.stride;
	},
	encode(_self, value: number) {
		const arr = new Uint8Array(8);
		new DataView(arr.buffer).setFloat64(0, value, true);
		return arr;
	},
	decode(_self, data) {
		const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
		return [view.getFloat64(0, true), 8];
	},
} satisfies Impl<F64, Codec<F64, number>>;
