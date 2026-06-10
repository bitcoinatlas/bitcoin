import { Codec, type Stride, U32LE } from "@nomadshiba/codec";
import { LockTime } from "~/lib/codec/LockTime.ts";

export type LockTimeVersionPack = { lockTime: LockTime; version: number };

/**
 * 1-byte tag folding the common (version, locktime-present) combinations.
 * The dominant case (v1/v2 + no locktime) stores ZERO payload bytes -- just the
 * tag. Other cases carry only what they need.
 *
 *   0 RAW      -> explicit version + locktime   (U32LE version + LockTime)
 *   1 V1_NONE  -> version 1, locktime none        (no payload)
 *   2 V2_NONE  -> version 2, locktime none        (no payload)
 *   3 V1_SOME  -> version 1, locktime set          (LockTime payload)
 *   4 V2_SOME  -> version 2, locktime set          (LockTime payload)
 */
const TAG_RAW = 0;
const TAG_V1_NONE = 1;
const TAG_V2_NONE = 2;
const TAG_V1_SOME = 3;
const TAG_V2_SOME = 4;

const NONE: LockTime = { kind: "none" };

/**
 * Codec presenting a flat { lockTime, version } on both encode and decode.
 * Hand-rolled: writes a 1-byte tag then only the bytes that variant needs.
 */
export class LockTimeVersionPackCodec extends Codec<LockTimeVersionPack> {
	public readonly stride: Stride<"variable"> = { kind: "variable" };

	public encode(value: LockTimeVersionPack, target?: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
		const { version, lockTime } = value;
		const noLock = lockTime.kind === "none";

		// Common, zero-payload cases: a single tag byte.
		if (noLock && version === 0x1) return writeTagOnly(TAG_V1_NONE, target);
		if (noLock && version === 0x2) return writeTagOnly(TAG_V2_NONE, target);

		// locktime-set with common version: tag + 4-byte LockTime.
		if (!noLock && version === 0x1) return writeTagPlusLockTime(TAG_V1_SOME, lockTime, target);
		if (!noLock && version === 0x2) return writeTagPlusLockTime(TAG_V2_SOME, lockTime, target);

		// Anything else: explicit version + locktime.
		const out = target ?? new Uint8Array(1 + 4 + 4);
		out[0] = TAG_RAW;
		out.set(U32LE.encode(version), 1);
		out.set(LockTime.encode(lockTime), 5);
		return out;
	}

	public decode(data: Uint8Array): [LockTimeVersionPack, number] {
		const tag = data[0]!;
		switch (tag) {
			case TAG_V1_NONE:
				return [{ lockTime: NONE, version: 0x1 }, 1];
			case TAG_V2_NONE:
				return [{ lockTime: NONE, version: 0x2 }, 1];
			case TAG_V1_SOME: {
				const [lockTime, size] = LockTime.decode(data.subarray(1));
				return [{ lockTime, version: 0x1 }, 1 + size];
			}
			case TAG_V2_SOME: {
				const [lockTime, size] = LockTime.decode(data.subarray(1));
				return [{ lockTime, version: 0x2 }, 1 + size];
			}
			case TAG_RAW: {
				const [version] = U32LE.decode(data.subarray(1));
				const [lockTime, ltSize] = LockTime.decode(data.subarray(5));
				return [{ lockTime, version }, 1 + 4 + ltSize];
			}
			default:
				throw new Error(`Unknown LockTimeVersionPack tag: ${tag}`);
		}
	}
}

function writeTagOnly(tag: number, target?: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
	const out = target ?? new Uint8Array(1);
	out[0] = tag;
	return out;
}

function writeTagPlusLockTime(
	tag: number,
	lockTime: LockTime,
	target?: Uint8Array<ArrayBuffer>,
): Uint8Array<ArrayBuffer> {
	const out = target ?? new Uint8Array(1 + 4);
	out[0] = tag;
	out.set(LockTime.encode(lockTime), 1);
	return out;
}

export const LockTimeVersionPack = new LockTimeVersionPackCodec();
