import { sha256 } from "@noble/hashes/sha2";
import { BytesCodec, Codec, u32LE, u64LE, varint } from "@nomadshiba/codec";
import { bytes32 } from "~/lib/codec/primitives.ts";
import { SequenceLock } from "~/lib/codec/weirdness/SequenceLock.ts";
import { TimeLock } from "~/lib/codec/weirdness/TimeLock.ts";

export type TimelineEntry = {
	version: number;
	vin: TimelineEntryIn[];
	vout: TimelineEntryOut[];
	lockTime: TimeLock;
	witness: boolean;
};
export type TimelineEntryWithId = TimelineEntry & { entryId: Uint8Array };

export type TimelineEntryIn = {
	entryId: Uint8Array;
	vout: number;
	scriptSig: Uint8Array;
	sequenceLock: SequenceLock;
	witness: Uint8Array[];
};

export type TimelineEntryOut = {
	value: bigint;
	scriptPubKey: Uint8Array;
};

const scriptBytes = new BytesCodec();
const witnessItemBytes = new BytesCodec();

export class TimelineEntryCodec extends Codec<TimelineEntry> {
	readonly stride = -1;

	encode(entry: TimelineEntry): Uint8Array {
		const chunks: Uint8Array[] = [];

		chunks.push(u32LE.encode(entry.version));

		const hasWitness = entry.witness && entry.vin.some((v) => v.witness.length > 0);
		if (hasWitness) {
			chunks.push(Uint8Array.of(0x00, 0x01));
		}

		chunks.push(varint.encode(entry.vin.length));
		for (const vin of entry.vin) {
			chunks.push(bytes32.encode(vin.entryId));
			chunks.push(u32LE.encode(vin.vout));
			chunks.push(scriptBytes.encode(vin.scriptSig));
			chunks.push(u32LE.encode(SequenceLock.encode(vin.sequenceLock)));
		}

		chunks.push(varint.encode(entry.vout.length));
		for (const vout of entry.vout) {
			chunks.push(u64LE.encode(vout.value));
			chunks.push(scriptBytes.encode(vout.scriptPubKey));
		}

		if (hasWitness) {
			for (const vin of entry.vin) {
				chunks.push(varint.encode(vin.witness.length));
				for (const item of vin.witness) {
					chunks.push(witnessItemBytes.encode(item));
				}
			}
		}

		chunks.push(u32LE.encode(TimeLock.encode(entry.lockTime)));

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

	decode(bytes: Uint8Array): [TimelineEntryWithId, number] {
		let offset = 0;

		const [version] = u32LE.decode(bytes.subarray(offset));
		offset += 4;

		let hasWitness = false;

		let [vinCount] = varint.decode(bytes.subarray(offset));
		let vinCountBytes: number;
		{
			const [, br] = varint.decode(bytes.subarray(offset));
			vinCountBytes = br;
		}

		if (vinCount === 0) {
			const flags = bytes[offset + vinCountBytes] ?? 0;
			offset += 1;
			if (flags !== 0) {
				const [vc, vcBytes] = varint.decode(bytes.subarray(offset));
				vinCount = vc;
				offset += vcBytes;
				hasWitness = (flags & 1) !== 0;
			}
		} else {
			offset += vinCountBytes;
		}

		const vin: TimelineEntryIn[] = [];
		for (let i = 0; i < vinCount; i++) {
			const [entryId] = bytes32.decode(bytes.subarray(offset));
			offset += 32;

			const [vout] = u32LE.decode(bytes.subarray(offset));
			offset += 4;

			const [scriptSig, scriptBytesRead] = scriptBytes.decode(bytes.subarray(offset));
			offset += scriptBytesRead;

			const [sequence] = u32LE.decode(bytes.subarray(offset));
			offset += 4;

			vin.push({
				entryId,
				vout,
				scriptSig,
				sequenceLock: SequenceLock.decode(sequence),
				witness: [],
			});
		}

		const [voutCount, voutCountBytes] = varint.decode(bytes.subarray(offset));
		offset += voutCountBytes;

		const vout: TimelineEntryOut[] = [];
		for (let i = 0; i < voutCount; i++) {
			const [value] = u64LE.decode(bytes.subarray(offset));
			offset += 8;

			const [scriptPubKey, pkBytesRead] = scriptBytes.decode(bytes.subarray(offset));
			offset += pkBytesRead;

			vout.push({ value, scriptPubKey });
		}

		if (hasWitness) {
			for (let i = 0; i < vinCount; i++) {
				const [nItems, nItemsBytes] = varint.decode(bytes.subarray(offset));
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

		const entry: TimelineEntryWithId = {
			entryId: sha256(sha256(bytes.subarray(0, offset))),
			version,
			vin,
			vout,
			lockTime: TimeLock.decode(locktime),
			witness: hasWitness,
		};

		return [entry, offset];
	}
}

export const timelineEntry = new TimelineEntryCodec();
