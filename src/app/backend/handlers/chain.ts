import { decodeHex } from "@std/encoding";
import { endpointRouter } from "~/app/router.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { chainStorage } from "~/chain/ChainStorage.ts";

const MAX_TAKE = 210;
// Each tx reconstructed by StoredTx.toWire does synchronous random reads (prevout
// txids + pubkey expansion), which block the event loop. Cap how many a single
// request may touch so one fat block can't freeze the whole server.
const MAX_TX_TAKE = 50;

endpointRouter.registerHandler("GET /v1/block", async ({ params }) => {
	const from = "from" in params.search ? Number(params.search["from"]) : undefined;
	const to = "to" in params.search ? Number(params.search["to"]) : undefined;
	if (from !== undefined && to !== undefined) {
		return { status: "BadRequest", message: "Cannot specify both 'from' and 'to' parameters" };
	}
	if (from !== undefined && isNaN(from)) {
		return { status: "BadRequest", message: "Invalid 'from' parameter" };
	}
	if (to !== undefined && isNaN(to)) {
		return { status: "BadRequest", message: "Invalid 'to' parameter" };
	}
	const take = Math.min(MAX_TAKE, "take" in params.search ? Number(params.search["take"]) : MAX_TAKE);
	if (isNaN(take) || take <= 0) {
		return { status: "BadRequest", message: "Invalid 'take' parameter" };
	}

	if (from !== undefined) {
		return { status: "OK", data: await chainStorage.getHeaderByRangeAsync(Math.max(0, from), from + take - 1) };
	}
	if (to !== undefined) {
		return { status: "OK", data: await chainStorage.getHeaderByRangeAsync(Math.max(0, to - take + 1), to) };
	}
	return { status: "OK", data: [] };
});

endpointRouter.registerHandler("GET /v1/block/tip", async () => {
	const tip = await chainStorage.getChainTipAsync();
	if (!tip) return { status: "OK", data: null };
	return { status: "OK", data: { height: tip.height, header: tip.header } };
});

function parseHashOrHeight(raw: string): { kind: "height"; height: number } | { kind: "hash"; hash: Uint8Array } {
	if (raw.length === 64) {
		return { kind: "hash", hash: Uint8Array.from(decodeHex(raw).reverse()) };
	}
	return { kind: "height", height: Number(raw) };
}

function resolveHeight(raw: string): number | undefined {
	const parsed = parseHashOrHeight(raw);
	if (parsed.kind === "height") return Number.isInteger(parsed.height) ? parsed.height : undefined;
	return chainStorage.getHeightByHash(parsed.hash);
}

endpointRouter.registerHandler("GET /v1/block/:hashOrHeight", async ({ params }) => {
	const height = resolveHeight(params.pathname.hashOrHeight);
	if (height === undefined) return { status: "OK", data: null };
	const header = await chainStorage.getHeaderByHeightAsync(height);
	if (!header) return { status: "OK", data: null };
	return { status: "OK", data: { height, header } };
});

endpointRouter.registerHandler("GET /v1/block/:hashOrHeight/summary", async ({ params }) => {
	const height = resolveHeight(params.pathname.hashOrHeight);
	if (height === undefined) return { status: "OK", data: null };

	const txs = await chainStorage.getTxsByBlockHeightAsync(height);
	if (!txs || txs.length === 0) return { status: "OK", data: null };

	const coinbase = txs[0]!;
	let reward = 0n;
	for (const output of coinbase.outputs) reward += output.value;

	return {
		status: "OK",
		data: {
			txCount: txs.length,
			reward,
			coinbaseScriptSig: coinbase.inputs[0]?.scriptSig ?? new Uint8Array(),
		},
	};
});

endpointRouter.registerHandler("GET /v1/block/:hashOrHeight/txs", async ({ params }) => {
	const height = resolveHeight(params.pathname.hashOrHeight);
	if (height === undefined) return { status: "OK", data: [] };
	const txs = await chainStorage.getTxsByBlockHeightAsync(height);
	if (!txs) return { status: "OK", data: [] };

	const fromRaw = "from" in params.search ? Number(params.search["from"]) : 0;
	const takeRaw = "take" in params.search ? Number(params.search["take"]) : MAX_TX_TAKE;
	const from = Number.isFinite(fromRaw) ? Math.max(0, Math.trunc(fromRaw)) : 0;
	const take = Number.isFinite(takeRaw) && takeRaw > 0 ? Math.min(MAX_TX_TAKE, Math.trunc(takeRaw)) : MAX_TX_TAKE;

	// Only reconstruct the requested window — toWire is O(inputs + outputs) random reads per tx.
	const slice = txs.slice(from, from + take);
	return { status: "OK", data: slice.map((tx) => StoredTx.toWire(tx, chainStorage)) };
});

endpointRouter.registerHandler("GET /v1/tx/:txId", async ({ params }) => {
	const txId = Uint8Array.from(decodeHex(params.pathname.txId).reverse());
	const tx = await chainStorage.getTxByIdAsync(txId);
	if (!tx) return { status: "OK", data: null };
	return { status: "OK", data: StoredTx.toWire(tx, chainStorage) };
});
