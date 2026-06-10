import { Codec, Stride } from "@nomadshiba/codec";
import { TxOutput } from "~/lib/chain/TxOutput.ts";
import {
        EXPECTED_SCRIPT_PAYLOAD_LEN,
        normalizeScriptPubKey,
        parseScriptPubKey,
        rawScriptPubKey,
        SCRIPTPUBKEY_PATTERN,
} from "~/lib/chain/ScriptPubKey.ts";
import { StoredPointer } from "~/lib/codec/stored/StoredPointer.ts";
import { VarInt } from "~/lib/codec/primitives.ts";

/**
 * StoredTxOutput binary layout
 *
 * Field order is chosen so all fixed-width / flag-controlled fields come first
 * and the only variable-length tail (script payload) comes last. This keeps the
 * layout dense - no padding, and the decoder advances a single cursor.
 *
 * -- 1-byte flag --
 * bits 0-3 : scriptTypeId  (4-bit script type, range 0..15)
 * bit  4   : spent         (1 = spentBy pointer follows, 0 = unspent, no pointer)
 * bits 5-7 : spare
 *
 * scriptTypeId mapping:
 *   0 = pointer, 1 = raw, 2 = p2pkh, 3 = p2sh, 4 = p2wpkh, 5 = p2wsh, 6 = p2tr
 *
 * -- value (variable) --
 *   VarInt-encoded satoshis (51-bit max; <= 7 bytes for any valid value)
 *
 * -- spentBy (conditional, fixed 6 bytes) --
 *   present ONLY when spent bit is set. StoredPointer (u48) blob offset of the
 *   spending input. Omitted entirely for unspent outputs (no 0-sentinel stored).
 *
 * -- script payload (variable, LAST) --
 *   pointer: 6 bytes (StoredPointer u48)
 *   raw:     VarInt-prefixed full script bytes
 *   known:   fixed-length hash bytes (20 or 32)
 */

export type StoredScriptPubKey =
        | { kind: "pointer"; value: number }
        | { kind: "raw" | keyof typeof SCRIPTPUBKEY_PATTERN; value: Uint8Array };

const SCRIPT_KIND_TO_ID: Record<StoredScriptPubKey["kind"], number> = {
        pointer: 0,
        raw: 1,
        p2pkh: 2,
        p2sh: 3,
        p2wpkh: 4,
        p2wsh: 5,
        p2tr: 6,
};

const SCRIPT_ID_TO_KIND: Record<number, StoredScriptPubKey["kind"]> = {
        0: "pointer",
        1: "raw",
        2: "p2pkh",
        3: "p2sh",
        4: "p2wpkh",
        5: "p2wsh",
        6: "p2tr",
};

const SCRIPT_TYPE_MASK = 0x0f;
const SPENT_BIT = 1 << 4;

function encodeScriptPubKey(data: StoredScriptPubKey): { kind: StoredScriptPubKey["kind"]; payload: Uint8Array } {
        if (data.kind === "pointer") {
                return { kind: "pointer", payload: StoredPointer.encode(data.value) };
        }

        const normalized = normalizeScriptPubKey(data);

        if (normalized.kind === "raw") {
                const script = rawScriptPubKey(normalized);
                const len = VarInt.encode(script.length);
                const payload = new Uint8Array(len.length + script.length);
                payload.set(len, 0);
                payload.set(script, len.length);
                return { kind: "raw", payload };
        }

        return { kind: normalized.kind, payload: normalized.value };
}

function decodeScriptPubKey(kind: StoredScriptPubKey["kind"], payload: Uint8Array): [StoredScriptPubKey, number] {
        if (kind === "pointer") {
                const [pointer, size] = StoredPointer.decode(payload);
                return [{ kind: "pointer", value: pointer }, size];
        }

        if (kind === "raw") {
                const [scriptLen, lenSize] = VarInt.decode(payload);
                const scriptBytes = payload.subarray(lenSize, lenSize + scriptLen);
                const decoded = parseScriptPubKey(scriptBytes);
                return [decoded, lenSize + scriptLen];
        }

        const expectedLen = EXPECTED_SCRIPT_PAYLOAD_LEN[kind];
        if (expectedLen === undefined) throw new Error(`Unknown script kind: ${kind}`);

        return [{ kind, value: payload.subarray(0, expectedLen) }, expectedLen];
}

export class StoredTxOutputCodec extends Codec<TxOutput> {
        readonly stride: Stride<"variable"> = { kind: "variable" };

        encode(output: TxOutput): Uint8Array<ArrayBuffer> {
                const { value, spent, spentBy, scriptPubKey } = output;

                if (value < 0n || value >= (1n << 51n)) {
                        throw new Error("Value out of range for 51-bit integer");
                }
                if (spent && (spentBy === undefined || spentBy === null)) {
                        throw new Error("spent output must carry a spentBy pointer");
                }

                const { kind, payload } = encodeScriptPubKey(scriptPubKey as StoredScriptPubKey);

                let flag = SCRIPT_KIND_TO_ID[kind] & SCRIPT_TYPE_MASK;
                if (spent) flag |= SPENT_BIT;

                const valueEncoded = VarInt.encode(Number(value));
                const spentByEncoded = spent ? StoredPointer.encode(spentBy!) : null;

                const total = 1 + valueEncoded.length +
                        (spentByEncoded ? spentByEncoded.length : 0) +
                        payload.length;

                const out = new Uint8Array(total);
                let offset = 0;

                out[offset] = flag;
                offset += 1;

                out.set(valueEncoded, offset);
                offset += valueEncoded.length;

                if (spentByEncoded) {
                        out.set(spentByEncoded, offset);
                        offset += spentByEncoded.length;
                }

                out.set(payload, offset);
                return out;
        }

        decode(bytes: Uint8Array): [TxOutput, number] {
                if (bytes.length < 1) throw new Error("Invalid data length for StoredTxOutput");

                let offset = 0;
                const flag = bytes[offset]!;
                offset += 1;

                const scriptTypeId = flag & SCRIPT_TYPE_MASK;
                const spent = (flag & SPENT_BIT) !== 0;

                const kind = SCRIPT_ID_TO_KIND[scriptTypeId];
                if (kind === undefined) throw new Error(`Unknown script type ID: ${scriptTypeId}`);

                const [valueNum, valueSize] = VarInt.decode(bytes.subarray(offset));
                offset += valueSize;
                const value = BigInt(valueNum);

                let spentBy: number | undefined;
                if (spent) {
                        const [ptr, ptrSize] = StoredPointer.decode(bytes.subarray(offset));
                        spentBy = ptr;
                        offset += ptrSize;
                }

                const [scriptPubKey, payloadSize] = decodeScriptPubKey(kind, bytes.subarray(offset));
                offset += payloadSize;

                const output: TxOutput = { value, spent, spentBy, scriptPubKey };
                return [output, offset];
        }
}

export const StoredTxOutput = new StoredTxOutputCodec();
