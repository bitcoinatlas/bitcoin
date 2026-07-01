import { Codec, Stride } from "@nomadshiba/codec";
import { StoredTxPointer } from "~/codec/stored/StoredTxPointer.ts";

export type StoredPrevOutTxId =
	| { kind: "pointer"; value: StoredTxPointer }
	| { kind: "coinbase"; value?: undefined };

/**
 * StoredPrevOutTxId binary layout
 *
 * Fixed 6-byte u48 slot:
 *   0 = coinbase
 *   otherwise pointer+1
 *
 * Fixed stride, always the first thing in a StoredTxInput encoding, so an
 * input's patch offset is simply the input's own offset. Use `patchPointer`
 * for deferred prevout resolution during parallel IBD — never write the slot
 * by hand, the +1 bias lives here and only here.
 *
 * Whether a VarInt vout follows (pointer) or not (coinbase) is decided by
 * the containing codec (StoredTxInput) based on the decoded kind.
 */

const COINBASE_SENTINEL = 0;

/**
 * Placeholder for prevouts whose pointer isn't resolved yet at encode time.
 * Nonzero on purpose: a placeholder that escapes patching must never decode
 * as coinbase — it should decode as a garbage pointer and fail loudly.
 */
export const PREVOUT_UNRESOLVED_PLACEHOLDER: StoredTxPointer = 2 ** 48 - 2;

export class StoredPrevOutTxIdCodec extends Codec<StoredPrevOutTxId> {
	readonly stride: Stride<"fixed"> = { kind: "fixed", size: StoredTxPointer.stride.size };

	/**
	 * Overwrite the pointer slot of an already-encoded StoredPrevOutTxId in
	 * place. `offset` is the offset of the slot (= the containing input's
	 * offset). Applies the +1 bias; pass the raw resolved pointer.
	 */
	static patchPointer(target: Uint8Array, offset: number, pointer: StoredTxPointer): void {
		StoredTxPointer.encodeInto(pointer + 1, target, offset);
	}

	encode(txId: StoredPrevOutTxId): Uint8Array<ArrayBuffer> {
		const result = new Uint8Array(this.stride.size);
		this.encodeInto(txId, result, 0);
		return result;
	}

	public override encodeInto(txId: StoredPrevOutTxId, target: Uint8Array, offset: number = 0): number {
		const { kind } = txId;

		if (kind === "pointer") {
			return StoredTxPointer.encodeInto(txId.value + 1, target, offset);
		}

		if (kind === "coinbase") {
			return StoredTxPointer.encodeInto(COINBASE_SENTINEL, target, offset);
		}

		throw new Error(`unknown txid kind, ${kind satisfies never}`);
	}

	decodeFrom(data: Uint8Array, offset: number): [StoredPrevOutTxId, number] {
		const [rawPointer] = StoredTxPointer.decodeFrom(data, offset);

		if (rawPointer === COINBASE_SENTINEL) {
			return [{ kind: "coinbase" }, this.stride.size];
		}

		return [{ kind: "pointer", value: rawPointer - 1 }, this.stride.size];
	}
}

export const StoredPrevOutTxId = new StoredPrevOutTxIdCodec();
