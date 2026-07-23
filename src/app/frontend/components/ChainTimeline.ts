import { api } from "~/app/frontend/api.ts";

export async function ChainTimeline() {
	const blocks = await api.fetch("GET /v1/block", {});
}
