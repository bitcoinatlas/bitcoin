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

	encode(value: TimeLock): Uint8Array {
		switch (value.kind) {
			case "none":
				return U32LE.encode(0);
			case "block": {
				if (value.height < 0 || value.height >= 500_000_000) {
					throw new RangeError("block height must be 0 … 499,999,999");
				}
				return U32LE.encode(value.height >>> 0);
			}
			case "time": {
				if (value.timestamp < 500_000_000 || value.timestamp > 0xffffffff) {
					throw new RangeError("timestamp must be ≥ 500,000,000 and fit in 32 bits");
				}
				return U32LE.encode(value.timestamp >>> 0);
			}
		}
	}

	decode(data: Uint8Array): [TimeLock, number] {
		const [locktime] = U32LE.decode(data);
		const value = locktime >>> 0;

		if (value === 0) return [{ kind: "none" }, 4];
		if (value < 500_000_000) return [{ kind: "block", height: value }, 4];
		return [{ kind: "time", timestamp: value }, 4];
	}
}

export const TimeLock = new TimeLockCodec();
