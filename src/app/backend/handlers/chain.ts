import { decodeHex } from "@std/encoding";
import { endpointRouter } from "~/app/router.ts";
import { StoredTx } from "~/codec/stored/StoredTx.ts";
import { chainStorage } from "~/chain/ChainStorage.ts";

const MAX_BLOCK_TAKE = 210;
const MAX_TX_TAKE = 50;

endpointRouter.registerHandler("GET /v1/block?from=:from&take=:take", async ({ params }) => {
	chainStorage.refresh();
	const from = Math.max(0, Number(params.search.from));
	if (isNaN(from)) {
		return { status: "BadRequest", message: "Invalid 'from' parameter" };
	}
	const take = Math.min(MAX_BLOCK_TAKE, Number(params.search.take));
	if (isNaN(take)) {
		return { status: "BadRequest", message: "Invalid 'take' parameter" };
	}
	return { status: "OK", data: await chainStorage.getHeaderByRangeAsync(from, from + take - 1) };
});

endpointRouter.registerHandler("GET /v1/block?to=:to&take=:take", async ({ params }) => {
	chainStorage.refresh();
	const to = Math.max(0, Number(params.search.to));
	if (isNaN(to)) {
		return { status: "BadRequest", message: "Invalid 'to' parameter" };
	}
	const take = Math.min(MAX_BLOCK_TAKE, Number(params.search.take));
	if (isNaN(take)) {
		return { status: "BadRequest", message: "Invalid 'take' parameter" };
	}
	return { status: "OK", data: await chainStorage.getHeaderByRangeAsync(Math.max(0, to - take + 1), to) };
});

endpointRouter.registerHandler("GET /v1/block/tip", async () => {
	chainStorage.refresh();
	const tip = await chainStorage.getChainTipAsync();
	if (!tip) return { status: "OK", data: null };
	return { status: "OK", data: tip };
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
	return { status: "OK", data: header };
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

	const fromRaw = params.search && "from" in params.search ? Number(params.search["from"]) : 0;
	const takeRaw = params.search && "take" in params.search ? Number(params.search["take"]) : MAX_TX_TAKE;
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
