import { delay } from "@std/async";
import { serve } from "~/app/serve.ts";
import { ChainStore } from "~/chain/ChainStore.ts";
import { registerEndpoints } from "~/app/backend/handlers/block.ts";

const global = globalThis as never as { gc?(): void };

if (import.meta.main) {
	Deno.addSignalListener("SIGINT", () => Deno.exit(0));
	if (Deno.args.length) {
		const timeout = Number(Deno.args[0]);
		setTimeout(() => Deno.exit(0), timeout * 1000);
	}

	serve(50021);

	const appWorker = new Worker(new URL("./app/gui.worker.ts", import.meta.url), { type: "module", name: "app" });
	const p2pWorker = new Worker(new URL("./p2p/worker.ts", import.meta.url), { type: "module", name: "p2p" });

	const chainStore = await ChainStore.open(p2pWorker);
	registerEndpoints(chainStore);

	let lastFlush: Promise<void> = Promise.resolve();
	while (true) {
		await delay(0);
		try {
			await tick();
		} catch (error) {
			console.error("[main] tick error:", error);
		}
	}

	function memcheck() {
		return (Deno.memoryUsage().heapUsed / (4 * 1024 * 1024 * 1024)) > .75;
	}

	async function tick() {
		// probably should call chain store sync or tick or something here?

		while (chainStore.atomic.busy && memcheck()) {
			if (global.gc) {
				const gcStart = performance.now();
				const pre = Deno.memoryUsage().heapUsed;
				global.gc();
				const post = Deno.memoryUsage().heapUsed;

				const mib = (bytes: number) => (bytes / 1024 / 1024).toFixed(2);
				const freed = pre - post;

				const gcEnd = performance.now();
				console.log(
					[
						`%cGC%c (${(gcEnd - gcStart).toFixed(0)}ms)`,
						`${mib(pre)} MiB %c→%c ${mib(post)} MiB %c(freed ${mib(freed)} MiB)`,
					].join(" "),
					"color: #888; font-weight: bold",
					"color: inherit",
					"color: #888",
					"color: inherit",
					freed > 0 ? "color: #4ade80" : "color: #f87171",
				);
			}
			if (memcheck()) {
				console.log("[main] heap usage almost at max even after gc, awaiting flush");
				await Promise.race([lastFlush, delay(30_000)]);
			}
		}

		if (chainStore.atomic.busy) return;
		lastFlush = chainStore.atomic.flush();
	}
}
