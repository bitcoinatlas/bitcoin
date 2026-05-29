import { decodeHex } from "@std/encoding";
import { endpointRouter } from "~/api/router.ts";
import {
	getBlockByHeight,
	getBlocksByHeightRange,
	getChainTip,
	getHeightByHash,
	getTxsByBlockHash,
	getTxsByBlockHeight,
} from "~/chain.ts";

const MAX_TAKE = 210;

endpointRouter.registerHandler("GET /v1/block", async ({ params }) => {
	const from = "from" in params.search ? Number(params.search["from"]) : undefined;
	const to = "to" in params.search ? Number(params.search["to"]) : undefined;
	if (from && to) {
		return { status: "BadRequest", error: "Cannot specify both 'from' and 'to' parameters" };
	}
	if (from && isNaN(from)) {
		return { status: "BadRequest", error: "Invalid 'from' parameter" };
	}
	if (to && isNaN(to)) {
		return { status: "BadRequest", error: "Invalid 'to' parameter" };
	}
	const take = Math.min(MAX_TAKE, "take" in params.search ? Number(params.search["take"]) : MAX_TAKE);
	if (isNaN(take) || take <= 0) {
		return { status: "BadRequest", error: "Invalid 'take' parameter" };
	}

	if (from) {
		const blocks = await getBlocksByHeightRange(from, from + take - 1);
		return { status: "OK", data: blocks };
	}

	if (to) {
		const blocks = await getBlocksByHeightRange(to - take + 1, to);
		return { status: "OK", data: blocks };
	}

	return { status: "OK", data: [] };
});

endpointRouter.registerHandler("GET /v1/block/tip", async () => {
	const tip = await getChainTip();
	if (!tip) {
		return { status: "OK", data: null };
	}
	return { status: "OK", data: { height: tip.height, header: tip.block.header } };
});

function parseHashOrHeight(raw: string): { kind: "height"; height: number } | { kind: "hash"; hash: Uint8Array } {
	if (raw.length === 64 && raw.startsWith("0")) {
		const hash = Uint8Array.from(decodeHex(raw).reverse());
		return { kind: "hash", hash };
	}
	const height = Number(raw);
	return { kind: "height", height };
}

endpointRouter.registerHandler("GET /v1/block/:hashOrHeight", async ({ params }) => {
	const parsed = parseHashOrHeight(params.pathname.hashOrHeight);
	let block;
	let height: number;

	if (parsed.kind === "height") {
		height = parsed.height;
		block = await getBlockByHeight(height);
	} else {
		const h = await getHeightByHash(parsed.hash);
		if (h === undefined) return { status: "OK", data: null };
		height = h;
		block = await getBlockByHeight(height);
	}

	if (!block) return { status: "OK", data: null };
	return { status: "OK", data: { height, header: block.header } };
});

endpointRouter.registerHandler("GET /v1/block/:hashOrHeight/txs", async ({ params }) => {
	const parsed = parseHashOrHeight(params.pathname.hashOrHeight);
	let txs;

	if (parsed.kind === "height") {
		txs = await getTxsByBlockHeight(parsed.height);
	} else {
		txs = await getTxsByBlockHash(parsed.hash);
	}

	if (!txs) return { status: "OK", data: [] };
	const wireTxs = await Promise.all(txs.map((tx) => tx.toWire()));
	return { status: "OK", data: wireTxs };
});
