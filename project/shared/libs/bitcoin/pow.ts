import { bytesToNumberLE } from "@noble/curves/abstract/utils";
import { WireBlockHeader } from "@project/codecs";

const TWO256 = 1n << 256n;
const TARGET_1 = nBitsToTarget(0x1d00ffff); // 0xffff * 256^26

function nBitsToTarget(nBits: number): bigint {
	const exponent = nBits >>> 24;
	const mantissa = nBits & 0x007fffff;
	return BigInt(mantissa) * (1n << (8n * (BigInt(exponent) - 3n)));
}

export function workFromHeader(header: WireBlockHeader): bigint {
	const target = nBitsToTarget(header.bits);
	return target > 0n ? (TWO256 / (target + 1n)) : 0n;
}

export function difficultyFromHeader(header: WireBlockHeader): number {
	const target = nBitsToTarget(header.bits);
	return target > 0n ? Number(TARGET_1 * 1_000_000n / target) / 1_000_000 : 0;
}

export function verifyProofOfWork(header: WireBlockHeader): boolean {
	const nBits = header.bits;
	const target = nBitsToTarget(nBits);
	const hashInt = bytesToNumberLE(header.hash()); // use LE since Bitcoin compares hashes as little-endian numbers
	return hashInt <= target;
}
