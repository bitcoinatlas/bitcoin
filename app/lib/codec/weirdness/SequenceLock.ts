export type SequenceLock =
	| { kind: "final" }
	| {
		kind: "disable";
		unused: number;
	}
	| {
		kind: "enable";
		relativeLock:
			| { kind: "commit"; commits: number }
			| { kind: "time"; seconds: number };
		unused: number;
	};

export namespace SequenceLock {
	export function decode(sequence: number): SequenceLock {
		sequence >>>= 0;

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
			: { kind: "commit" as const, commits: value };

		return {
			kind: "enable",
			relativeLock,
			unused,
		};
	}

	export function encode(lock: SequenceLock): number {
		if (lock.kind === "final") return 0xffffffff;

		if (lock.kind === "disable") {
			if (lock.unused >>> 31 !== 0) {
				throw new RangeError("disable.unused must fit in 31 bits");
			}
			return (1 << 31) | (lock.unused & 0x7fffffff);
		}

		let sequence = 0;

		let value: number;
		let typeFlag: boolean;

		if (lock.relativeLock.kind === "commit") {
			if (lock.relativeLock.commits < 0 || lock.relativeLock.commits > 0xffff) {
				throw new RangeError("commit count must fit in 16 bits");
			}
			value = lock.relativeLock.commits;
			typeFlag = false;
		} else {
			if (lock.relativeLock.seconds % 512 !== 0) {
				throw new RangeError("time-based lock must be a multiple of 512 seconds");
			}
			value = lock.relativeLock.seconds / 512;
			if (value < 0 || value > 0xffff) {
				throw new RangeError("time-based lock must fit in 16 bits (max ~389 days)");
			}
			typeFlag = true;
		}

		sequence |= value & 0xffff;
		if (typeFlag) sequence |= 1 << 22;

		if (lock.unused < 0 || lock.unused > 0x3fff) {
			throw new RangeError("enable.unused must fit in 14 bits");
		}

		const reservedLow = lock.unused & 0x3f;
		const reservedHigh = (lock.unused >>> 6) & 0xff;

		sequence |= reservedLow << 16;
		sequence |= reservedHigh << 23;

		return sequence >>> 0;
	}
}
