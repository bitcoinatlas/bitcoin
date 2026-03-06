import { FixedKVStore } from "~/lib/storage/FixedKVStore.ts";
import { DatabaseSync } from "node:sqlite";

const KEY_SIZE = 32;
const VALUE_SIZE = 128;
const TOTAL_ENTRIES = 5_000_000;
const READ_SAMPLES = 10_000;
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

function generateKeyValue(): { key: Uint8Array; value: Uint8Array } {
	const key = new Uint8Array(KEY_SIZE);
	const value = new Uint8Array(VALUE_SIZE);
	crypto.getRandomValues(key);
	crypto.getRandomValues(value);
	return { key, value };
}

async function benchmarkFixedKVStore1(keys: Uint8Array[], values: Uint8Array[]) {
	console.log("\n📊 FixedKVStore1");

	const store = new FixedKVStore("data/bench_kv1.db", {
		keyCodec: new FixedBytesCodec(KEY_SIZE),
		valueCodec: new FixedBytesCodec(VALUE_SIZE),
		blockCacheSize: 5000,
	});
	await store.prepare();

	// Writes
	console.log("  Writing...");
	const writeStart = performance.now();
	for (let i = 0; i < TOTAL_ENTRIES; i += BATCH_SIZE) {
		const batch = [];
		for (let j = i; j < Math.min(i + BATCH_SIZE, TOTAL_ENTRIES); j++) {
			batch.push({ key: keys[j]!, value: values[j]! });
		}
		await store.setMany(batch);

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
		await store.get(keys[i]!);
	}
	const readOps = READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`    ${readOps.toFixed(0)} ops/sec`);

	console.log("  Reading (batch)...");
	const batchReadStart = performance.now();
	for (let i = 0; i < READ_SAMPLES; i += 100) {
		await store.getMany(keys.slice(i, Math.min(i + 100, READ_SAMPLES)));
	}
	const batchReadOps = READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(`    ${batchReadOps.toFixed(0)} ops/sec`);

	await store.close();

	const stat = await Deno.stat("data/bench_kv1.db");
	console.log(`    File: ${(stat.size / 1024 / 1024).toFixed(2)} MB`);

	return { name: "FixedKVStore", writeOps, readOps, batchReadOps, fileSize: stat.size };
}

async function benchmarkSQLite(keys: Uint8Array[], values: Uint8Array[]) {
	console.log("\n📊 SQLite");
	const db = new DatabaseSync("data/bench_sqlite.db");
	db.exec(`PRAGMA journal_mode = WAL`);
	db.exec(`PRAGMA synchronous = NORMAL`);
	db.exec(`CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)`);

	// Writes
	console.log("  Writing...");
	const writeStart = performance.now();
	let lastLogTime = performance.now();
	for (let i = 0; i < TOTAL_ENTRIES; i += BATCH_SIZE) {
		db.exec("BEGIN");
		const stmt = db.prepare("INSERT INTO kv (key, value) VALUES (?, ?)");
		for (let j = i; j < Math.min(i + BATCH_SIZE, TOTAL_ENTRIES); j++) {
			stmt.run(keys[j]!, values[j]!);
		}
		db.exec("COMMIT");

		const now = performance.now();
		if (now - lastLogTime > 500) {
			const progress = ((i + BATCH_SIZE) / TOTAL_ENTRIES * 100).toFixed(1);
			const elapsed = (now - writeStart) / 1000;
			const rate = (i + BATCH_SIZE) / elapsed;
			console.log(`    ${progress}% - ${(i + BATCH_SIZE).toLocaleString()} entries (${rate.toFixed(0)} ops/sec)`);
			lastLogTime = now;
		}
	}
	const writeOps = TOTAL_ENTRIES / ((performance.now() - writeStart) / 1000);
	console.log(`    Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads
	console.log("  Reading (single)...");
	const readStart = performance.now();
	const selectStmt = db.prepare("SELECT value FROM kv WHERE key = ?");
	for (let i = 0; i < READ_SAMPLES; i++) {
		selectStmt.get(keys[i]!);
	}
	const readOps = READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`    ${readOps.toFixed(0)} ops/sec`);

	console.log("  Reading (batch)...");
	const batchReadStart = performance.now();
	const placeholders = new Array(100).fill("?").join(",");
	const batchStmt = db.prepare(`SELECT value FROM kv WHERE key IN (${placeholders})`);
	for (let i = 0; i < READ_SAMPLES; i += 100) {
		batchStmt.all(...keys.slice(i, Math.min(i + 100, READ_SAMPLES)));
	}
	const batchReadOps = READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(`    ${batchReadOps.toFixed(0)} ops/sec`);

	db.close();

	const stat = await Deno.stat("data/bench_sqlite.db");
	console.log(`    File: ${(stat.size / 1024 / 1024).toFixed(2)} MB`);

	return { name: "SQLite", writeOps, readOps, batchReadOps, fileSize: stat.size };
}

async function main() {
	// Cleanup
	await Deno.remove("data", { recursive: true }).catch(() => {});
	await Deno.mkdir("data", { recursive: true });

	console.log(`🔥 KV Store Benchmark`);
	console.log(`   Entries: ${TOTAL_ENTRIES.toLocaleString()}`);
	console.log(`   Key: ${KEY_SIZE}B, Value: ${VALUE_SIZE}B`);

	// Generate test data
	console.log("\n📦 Generating test data...");
	const keys: Uint8Array[] = [];
	const values: Uint8Array[] = [];
	for (let i = 0; i < TOTAL_ENTRIES; i++) {
		const { key, value } = generateKeyValue();
		keys.push(key);
		values.push(value);
	}

	// Run benchmarks
	const results = [];
	results.push(await benchmarkFixedKVStore1(keys, values));
	// results.push(await benchmarkSQLite(keys, values));

	// Summary
	console.log("\n" + "=".repeat(60));
	console.log("📊 RESULTS");
	console.log("=".repeat(60));
	console.log("Store              Writes      Single      Batch       Size");
	console.log("-".repeat(60));
	for (const r of results) {
		console.log(
			`${r.name.padEnd(17)} ${r.writeOps.toFixed(0).padStart(8)} ${r.readOps.toFixed(0).padStart(11)} ${
				r.batchReadOps.toFixed(0).padStart(11)
			} ${(r.fileSize / 1024 / 1024).toFixed(1).padStart(6)}MB`,
		);
	}

	// Cleanup
	await Deno.remove("data", { recursive: true }).catch(() => {});
}

main().catch(console.error);
