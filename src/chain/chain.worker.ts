import { ChainStore } from "~/chain/ChainStore.ts";

const consumers = new Array<Worker>(navigator.hardwareConcurrency);
for (let i = 0; i < consumers.length; i++) {
	const worker = new Worker(new URL("./consume.worker.ts", import.meta.url), { name: `consumer-${i}` });
	consumers[i] = worker;
}

self.addEventListener("message", async (event) => {
	const port = event.ports[0]!;
	const chainStore = ChainStore.start(port);
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
