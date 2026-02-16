// ── Stride ──

export type Stride = { value: number };

export const Stride = {
	fixed(value: number): Stride {
		return { value };
	},
	variable(): Stride {
		return { value: -1 };
	},
	isFixed(stride: Stride): boolean {
		return stride.value >= 0;
	},
	isVariable(stride: Stride): boolean {
		return stride.value < 0;
	},
};

// ── Codec trait ──
// Self is the schema (carries stride), T is the value type.

export type Codec<Self extends { stride: Stride } = { stride: Stride }, T = any> = {
	encode(self: Self, value: T): Uint8Array;
	decode(self: Self, data: Uint8Array): [T, number];
};
