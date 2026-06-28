import { sha256 } from "@noble/hashes/sha2";
import { StoredTxs } from "~/codec/stored/StoredTxs.ts";
import { FastUint8ArrayMap } from "~/libs/collections/FastUint8ArrayMap.ts";
import { rawScriptPubKey } from "~/chain/ScriptPubKey.ts";
import { StoredPointer } from "~/codec/stored/StoredPointer.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { MAX_BLOCK_SIZE, MAX_NON_WITNESS_BLOCK_SIZE } from "~/constants.ts";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U40 } from "~/codec/primitives/U40.ts";
import { formatHash } from "~/app/frontend/utils/format.ts";
import { WireTx } from "~/codec/wire/WireTx.ts";

// Consume Txs
const parts: Uint8Array[] = [];
self.addEventListener("message", (event) => {
	const buffer = event.data;
	parts.length = 0;
	console.log(`[chain] new chunk to consume size=${buffer.length}`);
	let offset = 0;
	while (offset < buffer.length) {
		const [txs, size] = StoredTxs.decode(buffer.subarray(offset));
		offset += size;
		parts.push(consume(txs));
	}

	self.postMessage(parts, parts); // done
});

function consume(txs: WireTx[]): void {
	try {
		// parse and encode the txs and block, push the parts
		// basic validation
		// in memory writes, fallback reads to the disk
	} catch (reason) {
		console.error("Failed to append txs:", reason);
		Deno.exit(1);
	}
}
