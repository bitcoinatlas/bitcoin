import { ARGS, BASE_DATA_DIR } from "~/env.ts";
import { atomic } from "~/chain/atomic.ts";
import { join } from "@std/path";

const global = globalThis as typeof globalThis & { gc?(): void };

if (import.meta.main) {
	Deno.addSignalListener("SIGINT", () => Deno.exit(0));

	await deleteTmpFiles(BASE_DATA_DIR);

	atomic.recover();
	const p2pWorker = new Worker(new URL("./p2p/worker.ts", import.meta.url), { type: "module", name: "p2p" });
	const chainWorker = new Worker(new URL("./chain/chain.worker.ts", import.meta.url), { type: "module", name: "chain" });

	const syncMessageChannel = new MessageChannel();
	Promise.all([
		new Promise((resolve) => chainWorker.addEventListener("message", resolve, { once: true })),
		new Promise((resolve) => p2pWorker.addEventListener("message", resolve, { once: true })),
	]).then(() => {
		p2pWorker.postMessage(null, [syncMessageChannel.port1]);
		chainWorker.postMessage(null, [syncMessageChannel.port2]);
	});

	// registerEndpoints(chainStore);
	// serve(58333);

	if (!ARGS.background) {
		new Worker(new URL("./app/gui.worker.ts", import.meta.url), { type: "module", name: "gui" });
	}
}

async function deleteTmpFiles(root: string): Promise<void> {
	for await (const entry of Deno.readDir(root)) {
		const path = join(root, entry.name);
		if (entry.isDirectory) {
			await deleteTmpFiles(path);
		} else if (entry.isFile && entry.name.endsWith(".tmp")) {
			await Deno.remove(path);
		}
	}
}
