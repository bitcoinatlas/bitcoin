import { Codec, U32LE } from "@nomadshiba/codec";

export type SequenceLock =
	| { kind: "final" }
	| {
		kind: "disable";
		unused: number;
	}
	| {
		kind: "enable";
		relativeLock:
			| { kind: "block"; blocks: number }
			| { kind: "time"; seconds: number };
		unused: number;
	};

// Wire-format codec for SequenceLock
// Encodes as U32LE using Bitcoin's sequence number encoding:
// - 0xffffffff = final
// - bit 31 set = disable flag
// - bit 22 set = time-based (seconds), otherwise block-based
export class SequenceLockCodec extends Codec<SequenceLock> {
	readonly stride = 4;

	static toU32(value: SequenceLock): number {
		if (value.kind === "final") {
			return 0xffffffff;
		}

		if (value.kind === "disable") {
			if (value.unused >>> 31 !== 0) {
				throw new RangeError("disable.unused must fit in 31 bits");
			}
			return (1 << 31) | (value.unused & 0x7fffffff);
		}

		let sequence = 0;
		let valueNum: number;
		let typeFlag: boolean;

		if (value.relativeLock.kind === "block") {
			if (value.relativeLock.blocks < 0 || value.relativeLock.blocks > 0xffff) {
				throw new RangeError("block count must fit in 16 bits");
			}
			valueNum = value.relativeLock.blocks;
			typeFlag = false;
		} else {
			if (value.relativeLock.seconds % 512 !== 0) {
				throw new RangeError("time-based lock must be a multiple of 512 seconds");
			}
			valueNum = value.relativeLock.seconds / 512;
			if (valueNum < 0 || valueNum > 0xffff) {
				throw new RangeError("time-based lock must fit in 16 bits (max ~389 days)");
			}
			typeFlag = true;
		}

		sequence |= valueNum & 0xffff;
		if (typeFlag) sequence |= 1 << 22;

		if (value.unused < 0 || value.unused > 0x3fff) {
			throw new RangeError("enable.unused must fit in 14 bits");
		}

		const reservedLow = value.unused & 0x3f;
		const reservedHigh = (value.unused >>> 6) & 0xff;

		sequence |= reservedLow << 16;
		sequence |= reservedHigh << 23;

		return sequence >>> 0;
	}

	static fromU32(seq: number): SequenceLock {
		const sequence = seq >>> 0;

		if (sequence === 0xffffffff) {
			return { kind: "final" };
		}

		const disableFlag = !!(sequence & (1 << 31));

		if (disableFlag) {
			const unused = sequence & 0x7fffffff;
			return { kind: "disable", unused };
		}

		const typeFlag = !!(sequence & (1 << 22));
		const value = sequence & 0xffff;

		const reservedLow = (sequence >>> 16) & 0x3f;
		const reservedHigh = (sequence >>> 23) & 0xff;
		const unused = (reservedHigh << 6) | reservedLow;

		const relativeLock = typeFlag
			? { kind: "time" as const, seconds: value * 512 }
			: { kind: "block" as const, blocks: value };

		return {
			kind: "enable",
			relativeLock,
			unused,
		};
	}

	encode(value: SequenceLock): Uint8Array {
		return U32LE.encode(SequenceLockCodec.toU32(value));
	}

	decode(data: Uint8Array): [SequenceLock, number] {
		const [seq] = U32LE.decode(data);
		return [SequenceLockCodec.fromU32(seq), 4];
	}
}

export const SequenceLock = new SequenceLockCodec();
