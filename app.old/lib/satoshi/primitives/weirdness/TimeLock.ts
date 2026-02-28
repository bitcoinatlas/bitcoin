export type TimeLock =
	| { kind: "none" } // locktime = 0
	| { kind: "block"; height: number } // block height
	| { kind: "time"; timestamp: number }; // unix timestamp

export namespace TimeLock {
	export function decode(locktime: number): TimeLock {
		locktime >>>= 0; // force unsigned 32-bit

		if (locktime === 0) return { kind: "none" };
		if (locktime < 500_000_000) return { kind: "block", height: locktime };
		return { kind: "time", timestamp: locktime };
	}

	export function encode(abs: TimeLock): number {
		switch (abs.kind) {
			case "none":
				return 0;

			case "block":
				if (abs.height < 0 || abs.height >= 500_000_000) {
					throw new RangeError("block height must be 0 … 499,999,999");
				}
				return abs.height >>> 0;

			case "time":
				if (abs.timestamp < 500_000_000 || abs.timestamp > 0xffffffff) {
					throw new RangeError("timestamp must be ≥ 500,000,000 and fit in 32 bits");
				}
				return abs.timestamp >>> 0;
		}
	}
}
