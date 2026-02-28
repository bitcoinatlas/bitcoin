import { sha256 } from "@noble/hashes/sha2";
import { concatBytes } from "@noble/hashes/utils";
import { equals } from "@std/bytes";

export function computeSatoshiMerkleRoot(txIds: Uint8Array[]): Uint8Array {
	if (txIds.length === 0) return new Uint8Array(32);

	let hashes = txIds.map((x) => x);

	while (hashes.length > 1) {
		// detect mutation: identical siblings at the same level
		for (let i = 0; i + 1 < hashes.length; i += 2) {
			if (equals(hashes.at(i)!, hashes.at(i + 1)!)) {
				return new Uint8Array(); // reject mutated tree
			}
		}

		if (hashes.length % 2 === 1) {
			hashes.push(hashes.at(-1)!); // duplicate last
		}

		const next: Uint8Array[] = [];
		for (let i = 0; i < hashes.length; i += 2) {
			next.push(sha256(sha256(concatBytes(hashes.at(i)!, hashes.at(i + 1)!))));
		}
		hashes = next;
	}

	return hashes.at(0)!;
}
