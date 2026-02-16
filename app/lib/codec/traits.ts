import type { DefaultImpl } from "~/traits.ts";

// ── HasStride ──

export type HasStride<Self = any> = {
	stride(self: Self): number;
	isFixed(self: Self): boolean;
	isVariable(self: Self): boolean;
};

export const HasStrideDefaults = <Self extends { stride: number }>() =>
	({
		stride(self) {
			return self.stride;
		},
		isFixed(self) {
			return self.stride >= 0;
		},
		isVariable(self) {
			return self.stride < 0;
		},
	}) satisfies DefaultImpl<HasStride<Self>>;

// ── Codec ──

export type Codec<Self extends { stride: number } = { stride: number }, T = any> = HasStride<Self> & {
	encode(self: Self, value: T): Uint8Array;
	decode(self: Self, data: Uint8Array): [T, number];
};

export const CodecDefaults = <Self extends { stride: number }>() =>
	({ ...HasStrideDefaults<Self>() }) satisfies DefaultImpl<Codec<Self>>;

// ── InferCodecValue ──

export type InferCodecValue<ImplObj> = ImplObj extends { encode(self: any, value: infer T): Uint8Array } ? T : never;
