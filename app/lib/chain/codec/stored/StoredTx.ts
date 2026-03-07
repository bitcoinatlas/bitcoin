import { ArrayCodec, Codec, U32LE } from "@nomadshiba/codec";
import { Bytes32, CompactSize } from "~/lib/codec/primitives.ts";
import { StoredTxInput } from "~/lib/chain/codec/stored/StoredTxInput.ts";
import { StoredTxOutput } from "~/lib/chain/codec/stored/StoredTxOutput.ts";
import { Tx } from "~/lib/chain/Tx.ts";
import { TxInput } from "~/lib/chain/TxInput.ts";
import { TxOutput } from "~/lib/chain/TxOutput.ts";
import { TimeLockCodec } from "~/lib/chain/codec/TimeLock.ts";

// StoredTx binary layout (optimized for disk storage):
// - txId: 32 bytes (full hash)
// - version: 4 bytes (u32LE)
// - lockTime: 4 bytes (u32LE) - stored as raw number, converted to TimeLock on decode
// - vout[]: CompactSize count + StoredTxOutput[]
// - vin[]: CompactSize count + StoredTxInput[] (uses pointers for prevOut when resolved)

export class StoredTxCodec extends Codec<Tx> {
	readonly stride = -1;

	encode(tx: Tx): Uint8Array {
		throw new Error("StoredTx encoding requires txId computation - use wire format first");
	}

	decode(bytes: Uint8Array): [Tx, number] {
		let offset = 0;

		// txId (32 bytes)
		const [txId] = Bytes32.decode(bytes.subarray(offset));
		offset += 32;

		// version (4 bytes)
		const [version] = U32LE.decode(bytes.subarray(offset));
		offset += 4;

		// lockTime (4 bytes) - convert to TimeLock
		const [lockTimeRaw] = U32LE.decode(bytes.subarray(offset));
		offset += 4;
		const locktime = TimeLockCodec.fromU32(lockTimeRaw);

		// vout[] - use StoredTxOutput which decodes to TxOutput
		const [voutCount, voutCountBytes] = CompactSize.decode(bytes.subarray(offset));
		offset += voutCountBytes;

		const vout: TxOutput[] = [];
		for (let i = 0; i < voutCount; i++) {
			const [output, bytesRead] = StoredTxOutput.decode(bytes.subarray(offset));
			vout.push(output);
			offset += bytesRead;
		}

		// vin[] - use StoredTxInput which decodes to TxInput
		const [vinCount, vinCountBytes] = CompactSize.decode(bytes.subarray(offset));
		offset += vinCountBytes;

		const vin: TxInput[] = [];
		for (let i = 0; i < vinCount; i++) {
			const [input, bytesRead] = StoredTxInput.decode(bytes.subarray(offset));
			vin.push(input);
			offset += bytesRead;
		}

		// witness flag - stored format doesn't explicitly store this
		// We can infer from inputs having witness data
		const witness = vin.some((v) => v.data.witness.length > 0);

		const tx = new Tx({
			version,
			locktime,
			witness,
			inputs: vin,
			output: vout,
		});

		return [tx, offset];
	}
}

// Type alias for decoded data (the runtime Tx class instance)
export type StoredTx = Tx;

// Uppercase singleton export (codec convention)
export const StoredTx = new StoredTxCodec();
