import { parseArgs } from "@std/cli/parse-args";
import { registerEndpoints } from "~/app/backend/handlers/block.ts";
import { serve } from "~/app/serve.ts";
import { ChainStore } from "~/chain/ChainStore.ts";

if (import.meta.main) {
	Deno.addSignalListener("SIGINT", () => Deno.exit(0));
	const args = parseArgs(Deno.args, { boolean: ["background"] });

	const p2pWorker = new Worker(new URL("./p2p/worker.ts", import.meta.url), { type: "module", name: "p2p" });
	const chainStore = await ChainStore.start(p2pWorker);
	registerEndpoints(chainStore);
	serve(50021);

	if (!args.background) {
		const guiWorker = new Worker(new URL("./app/gui.worker.ts", import.meta.url), { type: "module", name: "gui" });
	}

	while (true) {
		try {
			await tick();
		} catch (error) {
			console.error("[main] tick error:", error);
		}
	}

	async function tick() {
		await chainStore.tick();
	}
}
