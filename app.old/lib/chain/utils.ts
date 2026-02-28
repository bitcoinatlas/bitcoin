import { bytesToNumberLE } from "@noble/curves/abstract/utils";
import { BlockHeader } from "~/lib/satoshi/primitives/BlockHeader.ts";

const TWO256 = 1n << 256n;

export function decodeNBitsFromHeader(header: Uint8Array): number {
	const nBitsOffset = BlockHeader.shape.version.stride +
		BlockHeader.shape.prevHash.stride +
		BlockHeader.shape.merkleRoot.stride +
		BlockHeader.shape.timestamp.stride;
	return (
		header[nBitsOffset]! |
		(header[nBitsOffset + 1]! << 8) |
		(header[nBitsOffset + 2]! << 16) |
		(header[nBitsOffset + 3]! << 24)
	) >>> 0;
}

export function nBitsToTarget(nBits: number): bigint {
	const exponent = nBits >>> 24;
	const mantissa = nBits & 0x007fffff;
	return BigInt(mantissa) * (1n << (8n * (BigInt(exponent) - 3n)));
}

export function workFromHeader(header: Uint8Array): bigint {
	const target = nBitsToTarget(decodeNBitsFromHeader(header));
	return target > 0n ? (TWO256 / (target + 1n)) : 0n;
}

export function verifyProofOfWork(header: Uint8Array, hash: Uint8Array): boolean {
	const nBits = decodeNBitsFromHeader(header);
	const target = nBitsToTarget(nBits);
	const hashInt = bytesToNumberLE(hash); // use LE since Bitcoin compares hashes as little-endian numbers
	return hashInt <= target;
}
