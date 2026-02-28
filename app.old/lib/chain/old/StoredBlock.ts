import type { Impl } from "~/traits.ts";
import type { Codec } from "~/lib/codec/traits.ts";
import { CodecDefaults } from "~/lib/codec/traits.ts";
import { U24 } from "~/lib/codec/bitcoin.ts";
import { concat } from "@std/bytes";
import type { Block } from "../../satoshi/primitives/Block.ts";
import { SequenceLock } from "../../satoshi/primitives/weirdness/SequenceLock.ts";
import { TimeLock } from "../../satoshi/primitives/weirdness/TimeLock.ts";
import { StoredCoinbaseTx } from "../old/StoredCoinbaseTx.ts";
import { StoredTx } from "../old/StoredTx.ts";
import type { StoredTxInput } from "../old/StoredTxInput.ts";
import type { StoredTxOutput } from "../old/StoredTxOutput.ts";

const u24Codec = U24.create();

export type StoredBlock = {
	coinbaseTx: StoredCoinbaseTx;
	txs: StoredTx[];
};

type StoredBlockCodec = { stride: number };

const StoredBlockCodec = {
	...CodecDefaults<StoredBlockCodec>(),
	create(): StoredBlockCodec {
		return { stride: -1 };
	},
	encode(_self, value: StoredBlock) {
		const lengthEncoded = U24.encode(u24Codec, value.txs.length);
		const coinbaseEncoded = StoredCoinbaseTx.encode(value.coinbaseTx);
		const txsEncoded = value.txs.values().map((tx) => StoredTx.encode(tx));
		return concat([lengthEncoded, coinbaseEncoded, ...txsEncoded]);
	},
	decode(_self, data: Uint8Array) {
		let offset = 0;

		const [txCount, txCountSize] = U24.decode(u24Codec, data.subarray(offset));
		offset += txCountSize;
		const [coinbase, coinbaseSize] = StoredCoinbaseTx.decode(data.subarray(offset));
		offset += coinbaseSize;

		const txs: StoredTx[] = [];
		for (let i = 0; i < txCount; i++) {
			const [tx, txBytes] = StoredTx.decode(data.subarray(offset));
			txs.push(tx);
			offset += txBytes;
		}
		return [{ coinbaseTx: coinbase, txs }, offset] as [StoredBlock, number];
	},
	fromBlock(_self: StoredBlockCodec, block: Block): StoredBlock {
		const [coinbaseTx, ...txs] = block.txs;

		if (!coinbaseTx) {
			throw new Error("Block has no transactions");
		}

		const storedTxs: StoredTx[] = [];
		for (const tx of txs) {
			storedTxs.push({
				txId: tx.txId,
				lockTime: TimeLock.encode(tx.lockTime),
				version: tx.version,
				vin: tx.vin.map((vin): StoredTxInput => ({
					kind: "unresolved",
					value: {
						prevOut: {
							txId: vin.txId,
							vout: vin.vout,
						},
						scriptSig: vin.scriptSig,
						sequence: SequenceLock.encode(vin.sequenceLock),
						witness: vin.witness,
					},
				})),
				vout: tx.vout.map((vout): StoredTxOutput => ({
					scriptType: "raw",
					scriptPubKey: vout.scriptPubKey,
					value: vout.value,
					spent: false,
				})),
			});
		}

		return {
			coinbaseTx: {
				txId: coinbaseTx.txId,
				lockTime: TimeLock.encode(coinbaseTx.lockTime),
				version: coinbaseTx.version,
				coinbase: coinbaseTx.vin[0]!.scriptSig,
				sequence: SequenceLock.encode(coinbaseTx.vin[0]!.sequenceLock),
				vout: coinbaseTx.vout.map((vout): StoredTxOutput => ({
					scriptType: "raw",
					scriptPubKey: vout.scriptPubKey,
					value: vout.value,
					spent: false,
				})),
			},
			txs: storedTxs,
		};
	},
} satisfies Impl<StoredBlockCodec, Codec<StoredBlockCodec, StoredBlock>>;

const _codec = StoredBlockCodec.create();

export const StoredBlock: BoundCodec<StoredBlock> & { fromBlock(block: Block): StoredBlock } = {
	stride: _codec.stride,
	encode(value: StoredBlock): Uint8Array {
		return StoredBlockCodec.encode(_codec, value);
	},
	decode(data: Uint8Array): [StoredBlock, number] {
		return StoredBlockCodec.decode(_codec, data);
	},
	fromBlock(block: Block): StoredBlock {
		return StoredBlockCodec.fromBlock(_codec, block);
	},
};
