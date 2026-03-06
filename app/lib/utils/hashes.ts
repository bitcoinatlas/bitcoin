import { sha256 } from "@noble/hashes/sha2";

export function hash64(buffer: Uint8Array): bigint {
	const hash = sha256(buffer);
	return new DataView(hash.buffer, hash.byteOffset, hash.byteLength).getBigUint64(0) || 1n;
}
