import { Codec, type Stride, U32LE } from "@nomadshiba/codec";
import { LockTime } from "~/codec/LockTime.ts";

export type LockTimeVersionPack = { locktime: LockTime; version: number };

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
 * Codec presenting a flat { locktime, version } on both encode and decode.
 * Hand-rolled: writes a 1-byte tag then only the bytes that variant needs.
 */
export class LockTimeVersionPackCodec extends Codec<LockTimeVersionPack> {
	public readonly stride: Stride<"variable"> = { kind: "variable" };

	public encoder(value: LockTimeVersionPack, target: undefined, offset: undefined): Uint8Array<ArrayBuffer>;
	public encoder(value: LockTimeVersionPack, target: Uint8Array, offset: number): number;
	public encoder(value: LockTimeVersionPack, target?: Uint8Array, offset?: number): Uint8Array<ArrayBuffer> | number {
		const { version, locktime } = value;
		const noLock = locktime.kind === "none";

		if (target === undefined) {
			if (noLock && version === 0x1) return Uint8Array.of(TAG_V1_NONE);
			if (noLock && version === 0x2) return Uint8Array.of(TAG_V2_NONE);
			if (!noLock && version === 0x1) {
				const out = new Uint8Array(1 + 4);
				out[0] = TAG_V1_SOME;
				LockTime.encodeInto(locktime, out, 1);
				return out;
			}
			if (!noLock && version === 0x2) {
				const out = new Uint8Array(1 + 4);
				out[0] = TAG_V2_SOME;
				LockTime.encodeInto(locktime, out, 1);
				return out;
			}
			const out = new Uint8Array(1 + 4 + 4);
			out[0] = TAG_RAW;
			U32LE.encodeInto(version, out, 1);
			LockTime.encodeInto(locktime, out, 5);
			return out;
		}

		offset = offset!;
		if (noLock && version === 0x1) {
			target[offset] = TAG_V1_NONE;
			return 1;
		}
		if (noLock && version === 0x2) {
			target[offset] = TAG_V2_NONE;
			return 1;
		}
		if (!noLock && (version === 0x1 || version === 0x2)) {
			target[offset] = version === 0x1 ? TAG_V1_SOME : TAG_V2_SOME;
			LockTime.encodeInto(locktime, target, offset + 1);
			return 1 + 4;
		}
		target[offset] = TAG_RAW;
		U32LE.encodeInto(version, target, offset + 1);
		LockTime.encodeInto(locktime, target, offset + 5);
		return 1 + 4 + 4;
	}

	public decoder(data: Uint8Array, offset: number): [LockTimeVersionPack, number] {
		const tag = data[offset]!;
		switch (tag) {
			case TAG_V1_NONE:
				return [{ locktime: NONE, version: 0x1 }, 1];
			case TAG_V2_NONE:
				return [{ locktime: NONE, version: 0x2 }, 1];
			case TAG_V1_SOME: {
				const [locktime, size] = LockTime.decode(data, offset + 1);
				return [{ locktime, version: 0x1 }, 1 + size];
			}
			case TAG_V2_SOME: {
				const [locktime, size] = LockTime.decode(data, offset + 1);
				return [{ locktime, version: 0x2 }, 1 + size];
			}
			case TAG_RAW: {
				const [version] = U32LE.decode(data, offset + 1);
				const [locktime, ltSize] = LockTime.decode(data, offset + 5);
				return [{ locktime, version }, 1 + 4 + ltSize];
			}
			default:
				throw new Error(`Unknown LockTimeVersionPack tag: ${tag}`);
		}
	}
}

export const LockTimeVersionPack = new LockTimeVersionPackCodec();
