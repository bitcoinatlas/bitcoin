import { Enum } from "../../traits.ts";

export type Stride = Enum<typeof Stride>;
export const Stride = {
	fixed(size: number) {
		return { type: "fixed", size } as const;
	},
	variable() {
		return { type: "variable" } as const;
	},
};

export type Codec<Self = any, T = any> = {
	stride(self: Self): Stride;
	encode(self: Self, value: T, destination?: Uint8Array): Uint8Array;
	decode(self: Self, data: Uint8Array): [T, number];
};
