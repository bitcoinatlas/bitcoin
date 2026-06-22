import { ChainStore } from "~/chain/ChainStore.ts";

self.addEventListener("message", async (event) => {
	const port = event.ports[0]!;
	const chainStore = await ChainStore.start(port);
	port.start();
	while (true) {
		try {
			await chainStore.tick();
		} catch (error) {
			console.error("[main] tick error:", error);
		}
	}
}, { once: true });
self.postMessage(null);
