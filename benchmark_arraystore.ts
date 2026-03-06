import { CachedArrayStore } from "~/lib/storage/CachedArrayStore.ts";

const ITEM_SIZE = 64;
const TOTAL_ENTRIES = 5_000_000;
const READ_SAMPLES = 100_000;
const BATCH_SIZE = 100_000;

// Fixed-size codec for Uint8Array
class FixedBytesCodec {
	readonly stride: number;

	constructor(size: number) {
		this.stride = size;
	}

	encode(value: Uint8Array): Uint8Array {
		return value;
	}

	decode(data: Uint8Array): [Uint8Array, number] {
		return [data.slice(0, this.stride), this.stride];
	}
}

const CODEC = new FixedBytesCodec(ITEM_SIZE);

function generateItem(n: number): Uint8Array {
	const item = new Uint8Array(ITEM_SIZE);
	// Mix of sequential and random-ish data
	const view = new DataView(item.buffer);
	view.setBigUint64(0, BigInt(n), true);
	for (let i = 8; i < ITEM_SIZE; i++) {
		item[i] = (n * 31 + i) % 256;
	}
	return item;
}

async function benchmarkCachedArrayStore1() {
	const filePath = "data/bench_array1.bin";
	const store = new CachedArrayStore(filePath, CODEC);
	await store.prepare();

	// Generate test data
	console.log("  Generating test data...");
	const items: Uint8Array[] = [];
	for (let i = 0; i < TOTAL_ENTRIES; i++) {
		items.push(generateItem(i));
	}

	// Writes
	console.log("  Writing...");
	const writeStart = performance.now();
	for (let i = 0; i < TOTAL_ENTRIES; i += BATCH_SIZE) {
		const batch = items.slice(i, Math.min(i + BATCH_SIZE, TOTAL_ENTRIES));
		await store.pushMany(batch);

		const progress = ((i + batch.length) / TOTAL_ENTRIES * 100).toFixed(1);
		const elapsed = (performance.now() - writeStart) / 1000;
		const rate = (i + batch.length) / elapsed;
		console.log(`    ${progress}% - ${(i + batch.length).toLocaleString()} entries (${rate.toFixed(0)} ops/sec)`);
	}
	const writeOps = TOTAL_ENTRIES / ((performance.now() - writeStart) / 1000);
	console.log(`    Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads
	console.log("  Reading (single)...");
	const readStart = performance.now();
	for (let i = 0; i < READ_SAMPLES; i++) {
		const idx = Math.floor(Math.random() * TOTAL_ENTRIES);
		await store.get(idx);
	}
	const readOps = READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`    ${readOps.toFixed(0)} ops/sec`);

	console.log("  Reading (batch/sequential)...");
	const batchReadStart = performance.now();
	for (let i = 0; i < READ_SAMPLES; i += 100) {
		const startIdx = Math.floor(Math.random() * (TOTAL_ENTRIES - 100));
		await store.getRange(startIdx, 100);
	}
	const batchReadOps = READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(`    ${batchReadOps.toFixed(0)} ops/sec`);

	await store.close();

	const stat = await Deno.stat(filePath);
	console.log(`    File: ${(stat.size / 1024 / 1024).toFixed(2)} MB`);

	return {
		name: "MemoryArrayStore",
		writeOps,
		readOps,
		batchReadOps,
		fileSize: stat.size,
		memoryUsage: "Full dataset in RAM",
	};
}

async function main() {
	// Cleanup
	await Deno.remove("data", { recursive: true }).catch(() => {});
	await Deno.mkdir("data", { recursive: true });

	console.log(`🔥 Array Store Benchmark`);
	console.log(`   Entries: ${TOTAL_ENTRIES.toLocaleString()}`);
	console.log(`   Item size: ${ITEM_SIZE}B`);
	console.log(`   Read samples: ${READ_SAMPLES.toLocaleString()}`);

	// Run benchmarks
	const results = [];

	console.log("\n📦 CachedArrayStore (Full in-memory)");
	results.push(await benchmarkCachedArrayStore1());

	// Summary table
	console.log("\n" + "=".repeat(85));
	console.log("📊 RESULTS");
	console.log("=".repeat(85));
	console.log("Store                Writes/sec   Random/sec   Batch/sec    Size      Memory");
	console.log("-".repeat(85));

	for (const r of results) {
		console.log(
			`${r.name.padEnd(19)} ` +
				`${r.writeOps.toFixed(0).padStart(12)} ` +
				`${r.readOps.toFixed(0).padStart(12)} ` +
				`${r.batchReadOps.toFixed(0).padStart(12)} ` +
				`${(r.fileSize / 1024 / 1024).toFixed(1).padStart(8)}MB ` +
				`${r.memoryUsage}`,
		);
	}

	// Cleanup
	await Deno.remove("data", { recursive: true }).catch(() => {});
}

main().catch(console.error);
