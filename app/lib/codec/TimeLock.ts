import { Codec, U32LE } from "@nomadshiba/codec";

export type TimeLock =
	| { kind: "none" }
	| { kind: "block"; height: number }
	| { kind: "time"; timestamp: number };

// Wire-format codec for TimeLock
// Encodes as U32LE where:
// - 0 = none
// - < 500_000_000 = block height
// - >= 500_000_000 = timestamp
export class TimeLockCodec extends Codec<TimeLock> {
	readonly stride = 4;

	static toU32(value: TimeLock): number {
		switch (value.kind) {
			case "none":
				return 0;
			case "block":
				return value.height;
			case "time":
				return value.timestamp;
		}
	}

	static fromU32(value: number): TimeLock {
		if (value === 0) return { kind: "none" };
		if (value < 500_000_000) return { kind: "block", height: value };
		return { kind: "time", timestamp: value };
	}

	encode(value: TimeLock): Uint8Array {
		return U32LE.encode(TimeLockCodec.toU32(value));
	}

	decode(data: Uint8Array): [TimeLock, number] {
		const [locktime] = U32LE.decode(data);
		const value = locktime >>> 0;
		return [TimeLockCodec.fromU32(value), 4];
	}
}

export const TimeLock = new TimeLockCodec();
