export type SequenceLock =
	| { kind: "final" }
	| {
		kind: "disable";
		unused: number; // relative lock bits + reserved (31 bits total)
	}
	| {
		kind: "enable";
		relativeLock:
			| { kind: "block"; blocks: number } // 16-bit block count
			| { kind: "time"; seconds: number }; // must be multiple of 512
		unused: number; // reserved bits only (14 bits)
	};

export namespace SequenceLock {
	export function decode(sequence: number): SequenceLock {
		sequence >>>= 0; // force unsigned 32-bit

		if (sequence === 0xffffffff) {
			return { kind: "final" };
		}

		const disableFlag = !!(sequence & (1 << 31));

		if (disableFlag) {
			// when disabled, keep the entire lower 31 bits as unused
			const unused = sequence & 0x7fffffff;
			return { kind: "disable", unused };
		}

		const typeFlag = !!(sequence & (1 << 22));
		const value = sequence & 0xffff;

		// pack reserved bits (16–21 and 23–30) into a single integer
		const reservedLow = (sequence >>> 16) & 0x3f; // bits 16–21
		const reservedHigh = (sequence >>> 23) & 0xff; // bits 23–30
		const unused = (reservedHigh << 6) | reservedLow; // 14 bits total

		const relativeLock = typeFlag
			? { kind: "time" as const, seconds: value * 512 }
			: { kind: "block" as const, blocks: value };

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

		// enable
		let sequence = 0;

		let value: number;
		let typeFlag: boolean;

		if (lock.relativeLock.kind === "block") {
			if (lock.relativeLock.blocks < 0 || lock.relativeLock.blocks > 0xffff) {
				throw new RangeError("block count must fit in 16 bits");
			}
			value = lock.relativeLock.blocks;
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

		// unpack unused back into its bit slots
		const reservedLow = lock.unused & 0x3f;
		const reservedHigh = (lock.unused >>> 6) & 0xff;

		sequence |= reservedLow << 16; // bits 16–21
		sequence |= reservedHigh << 23; // bits 23–30

		return sequence >>> 0;
	}
}
