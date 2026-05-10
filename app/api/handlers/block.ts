import { endpointRouter } from "~/api/router.ts";
import { getBlocksByHeightRange, getChainTip } from "~/chain.ts";

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
