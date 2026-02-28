import { U48 } from "~/lib/codec/mod.ts";
import type { BoundCodec } from "~/lib/codec/mod.ts";

// Global pointer to anything stored on the chain
export type StoredPointer = number;

const _codec = U48.create();

export const StoredPointer: BoundCodec<StoredPointer> = {
	stride: _codec.stride,
	encode(value: StoredPointer): Uint8Array {
		return U48.encode(_codec, value);
	},
	decode(data: Uint8Array): [StoredPointer, number] {
		return U48.decode(_codec, data);
	},
};
