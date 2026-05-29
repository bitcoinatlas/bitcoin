import { assertEquals } from "@std/assert";
import { StoredTxs } from "~/lib/codec/stored/StoredTxs.ts";
import { TxOutput } from "~/lib/chain/TxOutput.ts";
import { TxInput } from "~/lib/chain/TxInput.ts";

function makeTx(fill: number): import("~/lib/codec/stored/StoredTx.ts").StoredTx {
	return {
		txId: new Uint8Array(32).fill(fill),
		version: 1,
		lockTime: { kind: "none" },
		vout: [
			new TxOutput({
				value: BigInt(fill) * 1000n,
				spent: false,
				scriptPubKey: { kind: "p2pkh", value: new Uint8Array(20).fill(fill) },
			}),
		],
		vin: [
			new TxInput({
				prevOut: { txId: { kind: "coinbase" }, vout: 0xffffffff },
				scriptSig: new Uint8Array([fill]),
				sequence: { kind: "final" },
				witness: [],
			}),
		],
	};
}

Deno.test("StoredTxs roundtrip - empty array", () => {
	const txs: import("~/lib/codec/stored/StoredTx.ts").StoredTx[] = [];
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
