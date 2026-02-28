import { Enum } from "../../traits.ts";

export type StrideFixed = { type: "fixed"; size: number };
export type StrideVariable = { type: "variable" };

export type Stride = Enum<typeof Stride>;
export const Stride = {
	fixed(size: number): StrideFixed {
		return { type: "fixed", size };
	},
	variable(): StrideVariable {
		return { type: "variable" };
	},
};

export type Codec<Self = any, Item = any> = {
	stride(self: Self): Stride;
	encode(self: Self, item: Item, destination?: Uint8Array): Uint8Array;
	decode(self: Self, data: Uint8Array): [Item, number];
};
