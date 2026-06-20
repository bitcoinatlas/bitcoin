import { Codec, Stride, U32LE } from "@nomadshiba/codec";

export type LockTime =
	| { kind: "none" }
	| { kind: "block"; height: number }
	| { kind: "time"; timestamp: number };

// Wire-format codec for LockTime
// Encodes as U32LE where:
// - 0 = none
// - < 500_000_000 = block height
// - >= 500_000_000 = timestamp
export class LockTimeCodec extends Codec<LockTime> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: 4 };

	static toU32(value: LockTime): number {
		switch (value.kind) {
			case "none":
				return 0;
			case "block":
				return value.height;
			case "time":
				return value.timestamp;
		}
	}

	static fromU32(value: number): LockTime {
		if (value === 0) return { kind: "none" };
		if (value < 500_000_000) return { kind: "block", height: value };
		return { kind: "time", timestamp: value };
	}

	public encode(value: LockTime): Uint8Array<ArrayBuffer> {
		return U32LE.encode(LockTimeCodec.toU32(value));
	}

	public override encodeInto(value: LockTime, target: Uint8Array, offset: number = 0): number {
		return U32LE.encodeInto(LockTimeCodec.toU32(value), target, offset);
	}

	public decode(data: Uint8Array): [LockTime, number] {
		const [locktime] = U32LE.decode(data);
		const value = locktime >>> 0;
		return [LockTimeCodec.fromU32(value), 4];
	}
}

export const LockTime = new LockTimeCodec();
