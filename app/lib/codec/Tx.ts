import { sha256 } from "@noble/hashes/sha2";
import { BytesCodec, Codec, u32LE, u64LE } from "@nomadshiba/codec";
import { bytes32, compactSize } from "~/lib/codec/primitives.ts";
import { TimeLock } from "../chain/utils/TimeLock.ts";
import { SequenceLock } from "../chain/utils/SequenceLock.ts";
export type TxData = {
	version: number;
	vin: TxInputData[];
	vout: TxOutputData[];
	lockTime: TimeLock;
	witness: boolean;
};
export type TxWithIdData = TxData & { txId: Uint8Array };

export type TxInputData = {
	txId: Uint8Array;
	vout: number;
	scriptSig: Uint8Array;
	sequenceLock: SequenceLock;
	witness: Uint8Array[];
};

export type TxOutputData = {
	value: bigint;
	scriptPubKey: Uint8Array;
};

const scriptBytes = new BytesCodec(undefined, { lengthCodec: compactSize });
const witnessItemBytes = new BytesCodec(undefined, { lengthCodec: compactSize });

export class TxCodec extends Codec<TxData> {
	readonly stride = -1;

	encode(tx: TxData): Uint8Array {
		const chunks: Uint8Array[] = [];

		chunks.push(u32LE.encode(tx.version));

		const hasWitness = tx.witness && tx.vin.some((v) => v.witness.length > 0);
		if (hasWitness) {
			chunks.push(Uint8Array.of(0x00, 0x01));
		}

		chunks.push(compactSize.encode(tx.vin.length));
		for (const vin of tx.vin) {
			chunks.push(bytes32.encode(vin.txId));
			chunks.push(u32LE.encode(vin.vout));
			chunks.push(scriptBytes.encode(vin.scriptSig));
			chunks.push(u32LE.encode(SequenceLock.toU32(vin.sequenceLock)));
		}

		chunks.push(compactSize.encode(tx.vout.length));
		for (const vout of tx.vout) {
			chunks.push(u64LE.encode(vout.value));
			chunks.push(scriptBytes.encode(vout.scriptPubKey));
		}

		if (hasWitness) {
			for (const vin of tx.vin) {
				chunks.push(compactSize.encode(vin.witness.length));
				for (const item of vin.witness) {
					chunks.push(witnessItemBytes.encode(item));
				}
			}
		}

		chunks.push(u32LE.encode(TimeLock.toU32(tx.lockTime)));

		let totalLength = 0;
		for (const chunk of chunks) {
			totalLength += chunk.length;
		}
		const result = new Uint8Array(totalLength);
		let offset = 0;
		for (const chunk of chunks) {
			result.set(chunk, offset);
			offset += chunk.length;
		}
		return result;
	}

	decode(bytes: Uint8Array): [TxWithIdData, number] {
		let offset = 0;

		const [version] = u32LE.decode(bytes.subarray(offset));
		offset += 4;

		let hasWitness = false;

		let [vinCount] = compactSize.decode(bytes.subarray(offset));
		let vinCountBytes: number;
		{
			const [, br] = compactSize.decode(bytes.subarray(offset));
			vinCountBytes = br;
		}

		if (vinCount === 0) {
			const flags = bytes[offset + vinCountBytes] ?? 0;
			offset += 1;
			if (flags !== 0) {
				const [vc, vcBytes] = compactSize.decode(bytes.subarray(offset));
				vinCount = vc;
				offset += vcBytes;
				hasWitness = (flags & 1) !== 0;
			}
		} else {
			offset += vinCountBytes;
		}

		const vin: TxInputData[] = [];
		for (let i = 0; i < vinCount; i++) {
			const [txId] = bytes32.decode(bytes.subarray(offset));
			offset += 32;

			const [vout] = u32LE.decode(bytes.subarray(offset));
			offset += 4;

			const [scriptSig, scriptBytesRead] = scriptBytes.decode(bytes.subarray(offset));
			offset += scriptBytesRead;

			const [sequence] = u32LE.decode(bytes.subarray(offset));
			offset += 4;

			vin.push({
				txId,
				vout,
				scriptSig,
				sequenceLock: SequenceLock.fromU32(sequence),
				witness: [],
			});
		}

		const [voutCount, voutCountBytes] = compactSize.decode(bytes.subarray(offset));
		offset += voutCountBytes;

		const vout: TxOutputData[] = [];
		for (let i = 0; i < voutCount; i++) {
			const [value] = u64LE.decode(bytes.subarray(offset));
			offset += 8;

			const [scriptPubKey, pkBytesRead] = scriptBytes.decode(bytes.subarray(offset));
			offset += pkBytesRead;

			vout.push({ value, scriptPubKey });
		}

		if (hasWitness) {
			for (let i = 0; i < vinCount; i++) {
				const [nItems, nItemsBytes] = compactSize.decode(bytes.subarray(offset));
				offset += nItemsBytes;

				const witness = vin[i]!.witness as Uint8Array[];
				for (let j = 0; j < nItems; j++) {
					const [item, itemBytes] = witnessItemBytes.decode(bytes.subarray(offset));
					offset += itemBytes;
					witness.push(item);
				}
			}
		}

		const [locktime] = u32LE.decode(bytes.subarray(offset));
		offset += 4;

		const tx: TxWithIdData = {
			txId: sha256(sha256(bytes.subarray(0, offset))),
			version,
			vin,
			vout,
			lockTime: TimeLock.fromU32(locktime),
			witness: hasWitness,
		};

		return [tx, offset];
	}
}

export const txCodec = new TxCodec();
