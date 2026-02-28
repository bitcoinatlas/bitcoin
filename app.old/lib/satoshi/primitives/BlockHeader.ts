import { sha256 } from "@noble/hashes/sha2";
import { dyn } from "~/traits.ts";
import { I32, U32 } from "~/lib/codec/primitives.ts";
import { Bytes32 } from "~/lib/codec/bitcoin.ts";
import { StructCodec } from "~/lib/codec/composites.ts";

export type BlockHeader = Readonly<{
	hash: Uint8Array;
	version: number;
	prevHash: Uint8Array;
	merkleRoot: Uint8Array;
	timestamp: number;
	bits: number;
	nonce: number;
}>;

const i32 = I32.create();
const u32 = U32.create();
const bytes32 = Bytes32.create();

const innerCodec = StructCodec.create<Omit<BlockHeader, "hash">>({
	version: dyn(I32, i32),
	prevHash: dyn(Bytes32, bytes32),
	merkleRoot: dyn(Bytes32, bytes32),
	timestamp: dyn(U32, u32),
	bits: dyn(U32, u32),
	nonce: dyn(U32, u32),
});

export const BlockHeader = {
	stride: innerCodec.stride,
	shape: {
		version: { stride: i32.stride },
		prevHash: { stride: bytes32.stride },
		merkleRoot: { stride: bytes32.stride },
		timestamp: { stride: u32.stride },
		bits: { stride: u32.stride },
		nonce: { stride: u32.stride },
	},
	encode(value: BlockHeader | Omit<BlockHeader, "hash">): Uint8Array {
		return StructCodec.encode(innerCodec, value as Omit<BlockHeader, "hash">);
	},
	decode(bytes: Uint8Array): [BlockHeader, number] {
		if (bytes.length < innerCodec.stride) {
			throw new Error(
				`Not enough bytes to decode BlockHeader: need ${innerCodec.stride}, got ${bytes.length}`,
			);
		}
		const [header, bytesRead] = StructCodec.decode<Omit<BlockHeader, "hash">>(innerCodec, bytes);
		const hash = sha256(sha256(bytes.subarray(0, bytesRead)));
		return [{ ...header, hash }, bytesRead];
	},
};
