import { bytesToNumberLE } from "@noble/curves/abstract/utils";
import { WireBlockHeader } from "../codec/wire/WireBlockHeader.ts";

const TWO256 = 1n << 256n;

function nBitsToTarget(nBits: number): bigint {
	const exponent = nBits >>> 24;
	const mantissa = nBits & 0x007fffff;
	return BigInt(mantissa) * (1n << (8n * (BigInt(exponent) - 3n)));
}

export function workFromHeader(header: WireBlockHeader): bigint {
	const target = nBitsToTarget(header.bits);
	return target > 0n ? (TWO256 / (target + 1n)) : 0n;
}

export function verifyProofOfWork(header: WireBlockHeader): boolean {
	const nBits = header.bits;
	const target = nBitsToTarget(nBits);
	const hashInt = bytesToNumberLE(header.hash); // use LE since Bitcoin compares hashes as little-endian numbers
	return hashInt <= target;
}
