import { assertEquals } from "@std/assert";
import { StoredTxs } from "~/lib/codec/stored/StoredTxs.ts";
import type { TxInput } from "~/lib/chain/TxInput.ts";
import { TxOutput } from "~/lib/codec/stored/StoredTxOutput.ts";
import { StoredTx } from "~/lib/codec/stored/StoredTx.ts";

function makeTx(fill: number): StoredTx {
	const output: TxOutput = {
		value: BigInt(fill) * 1000n,
		spentBy: null,
		scriptPubKey: { kind: "p2pkh", value: new Uint8Array(20).fill(fill) },
	};
	const input: TxInput = {
		prevOut: { txId: { kind: "coinbase" }, vout: 0xffffffff },
		scriptSig: new Uint8Array([fill]),
		sequence: { kind: "final" },
		witness: [],
	};
	return {
		txId: new Uint8Array(32).fill(fill),
		version: 1,
		lockTime: { kind: "none" },
		vout: [output],
		vin: [input],
	};
}

Deno.test("StoredTxs roundtrip - empty array", () => {
	const txs: StoredTx[] = [];
	const [decoded] = StoredTxs.decode(StoredTxs.encode(txs));
	assertEquals(decoded.length, 0);
});

Deno.test("StoredTxs roundtrip - single tx", () => {
	const txs = [makeTx(0xaa)];
	const [decoded] = StoredTxs.decode(StoredTxs.encode(txs));
	assertEquals(decoded.length, 1);
	assertEquals(decoded[0]!.txId, txs[0]!.txId);
});

Deno.test("StoredTxs roundtrip - multiple txs", () => {
	const txs = [makeTx(0x01), makeTx(0x02), makeTx(0x03)];
	const [decoded] = StoredTxs.decode(StoredTxs.encode(txs));
	assertEquals(decoded.length, 3);
	for (let i = 0; i < txs.length; i++) {
		assertEquals(decoded[i]!.txId, txs[i]!.txId);
		assertEquals(decoded[i]!.version, txs[i]!.version);
	}
});

Deno.test("StoredTxs encode is deterministic", () => {
	const txs = [makeTx(0x55), makeTx(0x66)];
	const a = StoredTxs.encode(txs);
	const b = StoredTxs.encode(txs);
	assertEquals(a, b);
});
