import { sha256 } from "@noble/hashes/sha2";
import { BytesCodec, Codec, U32LE, U64LE } from "@nomadshiba/codec";
import { Bytes32, CompactSize } from "~/lib/codec/primitives.ts";
import { TimeLock, TimeLockCodec } from "./TimeLock.ts";
import { SequenceLock, SequenceLockCodec } from "./SequenceLock.ts";
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

const scriptBytes = new BytesCodec({ lengthCodec: CompactSize });
const witnessItemBytes = new BytesCodec({ lengthCodec: CompactSize });

export class TxCodec extends Codec<TxData> {
	readonly stride = -1;

	encode(tx: TxData): Uint8Array {
		const chunks: Uint8Array[] = [];

		chunks.push(U32LE.encode(tx.version));

		const hasWitness = tx.witness && tx.vin.some((v) => v.witness.length > 0);
		if (hasWitness) {
			chunks.push(Uint8Array.of(0x00, 0x01));
		}

		chunks.push(CompactSize.encode(tx.vin.length));
		for (const vin of tx.vin) {
			chunks.push(Bytes32.encode(vin.txId));
			chunks.push(U32LE.encode(vin.vout));
			chunks.push(scriptBytes.encode(vin.scriptSig));
			chunks.push(U32LE.encode(SequenceLockCodec.toU32(vin.sequenceLock)));
		}

		chunks.push(CompactSize.encode(tx.vout.length));
		for (const vout of tx.vout) {
			chunks.push(U64LE.encode(vout.value));
			chunks.push(scriptBytes.encode(vout.scriptPubKey));
		}

		if (hasWitness) {
			for (const vin of tx.vin) {
				chunks.push(CompactSize.encode(vin.witness.length));
				for (const item of vin.witness) {
					chunks.push(witnessItemBytes.encode(item));
				}
			}
		}

		chunks.push(U32LE.encode(TimeLockCodec.toU32(tx.lockTime)));

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

		const [version] = U32LE.decode(bytes.subarray(offset));
		offset += 4;

		let hasWitness = false;

		let [vinCount] = CompactSize.decode(bytes.subarray(offset));
		let vinCountBytes: number;
		{
			const [, br] = CompactSize.decode(bytes.subarray(offset));
			vinCountBytes = br;
		}

		if (vinCount === 0) {
			const flags = bytes[offset + vinCountBytes] ?? 0;
			offset += 1;
			if (flags !== 0) {
				const [vc, vcBytes] = CompactSize.decode(bytes.subarray(offset));
				vinCount = vc;
				offset += vcBytes;
				hasWitness = (flags & 1) !== 0;
			}
		} else {
			offset += vinCountBytes;
		}

		const vin: TxInputData[] = [];
		for (let i = 0; i < vinCount; i++) {
			const [txId] = Bytes32.decode(bytes.subarray(offset));
			offset += 32;

			const [vout] = U32LE.decode(bytes.subarray(offset));
			offset += 4;

			const [scriptSig, scriptBytesRead] = scriptBytes.decode(bytes.subarray(offset));
			offset += scriptBytesRead;

			const [sequence] = U32LE.decode(bytes.subarray(offset));
			offset += 4;

			vin.push({
				txId,
				vout,
				scriptSig,
				sequenceLock: SequenceLockCodec.fromU32(sequence),
				witness: [],
			});
		}

		const [voutCount, voutCountBytes] = CompactSize.decode(bytes.subarray(offset));
		offset += voutCountBytes;

		const vout: TxOutputData[] = [];
		for (let i = 0; i < voutCount; i++) {
			const [value] = U64LE.decode(bytes.subarray(offset));
			offset += 8;

			const [scriptPubKey, pkBytesRead] = scriptBytes.decode(bytes.subarray(offset));
			offset += pkBytesRead;

			vout.push({ value, scriptPubKey });
		}

		if (hasWitness) {
			for (let i = 0; i < vinCount; i++) {
				const [nItems, nItemsBytes] = CompactSize.decode(bytes.subarray(offset));
				offset += nItemsBytes;

				const witness = vin[i]!.witness as Uint8Array[];
				for (let j = 0; j < nItems; j++) {
					const [item, itemBytes] = witnessItemBytes.decode(bytes.subarray(offset));
					offset += itemBytes;
					witness.push(item);
				}
			}
		}

		const [locktime] = U32LE.decode(bytes.subarray(offset));
		offset += 4;

		const tx: TxWithIdData = {
			txId: sha256(sha256(bytes.subarray(0, offset))),
			version,
			vin,
			vout,
		lockTime: TimeLockCodec.fromU32(locktime),
			witness: hasWitness,
		};

		return [tx, offset];
	}
}

export const txCodec = new TxCodec();
