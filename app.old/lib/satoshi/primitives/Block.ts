import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { concatBytes } from "@noble/hashes/utils";
import { equals } from "@std/bytes";
import { CompactSize } from "~/lib/CompactSize.ts";
import { BlockHeader } from "~/lib/satoshi/primitives/BlockHeader.ts";
import { Tx } from "~/lib/satoshi/primitives/Tx.ts";
import { humanize } from "~/lib/logging/human.ts";

export type Block = Readonly<{
	header: BlockHeader;
	txs: Tx[];
}>;

export type BlockCodec = { stride: number };

export const BlockCodec = {
	...CodecDefaults<BlockCodec>(),
	create(): BlockCodec {
		return { stride: -1 };
	},
	encode(_self, block: Block) {
		const headerBytes = BlockHeader.encode(block.header);
		const countBytes = CompactSize.encode(block.txs.length);
		const txsBytes = block.txs.map((tx) => Tx.encode(tx));
		return concatBytes(headerBytes, countBytes, ...txsBytes);
	},
	decode(_self, bytes: Uint8Array) {
		let offset = 0;

		const [header, headerBytes] = BlockHeader.decode(bytes.subarray(offset));
		offset += headerBytes;

		const [txCount, off2] = CompactSize.decode(bytes, offset);
		offset = off2;

		const txs: Tx[] = [];
		for (let i = 0; i < txCount; i++) {
			const [tx, txBytesRead] = Tx.decode(bytes.subarray(offset));

			// TODO: Test, remove later
			const txBytes = bytes.subarray(offset, offset + txBytesRead);
			const txEncoded = Tx.encode(tx);
			if (!equals(txBytes, txEncoded)) {
				console.error("Original bytes:", humanize(txBytes));
				console.error("Re-encoded bytes:", humanize(txEncoded));
				throw new Error("Tx encoding/decoding mismatch");
			}

			txs.push(tx);
			offset += txBytesRead;
		}

		if (txs.length !== txCount) {
			throw new Error(`Transaction count mismatch: expected ${txCount}, got ${txs.length}`);
		}

		return [{ header, txs }, offset] as [Block, number];
	},
} satisfies Impl<BlockCodec, Codec<BlockCodec, Block>>;
