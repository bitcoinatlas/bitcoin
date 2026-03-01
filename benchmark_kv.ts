/// <reference lib="deno.unstable" />
import { RocksLite } from "~/lib/storage/RocksLite.ts";
import { DatabaseSync } from "node:sqlite";

const KEY_SIZE = 32;
const VALUE_SIZE = 128;
const TOTAL_ENTRIES = 200_000;
const READ_SAMPLES = 10_000;
const SAMPLE_INTERVAL = 5_000;

interface BenchmarkResult {
	name: string;
	writeOps: number;
	readOps: number;
	writeSamples: number[]; // ops/sec per chunk
	fileSize: number;
}

function generateKeyValue(): { key: Uint8Array; value: Uint8Array } {
	const key = new Uint8Array(KEY_SIZE);
	const value = new Uint8Array(VALUE_SIZE);
	crypto.getRandomValues(key);
	crypto.getRandomValues(value);
	return { key, value };
}

async function benchmarkStore(
	name: string,
	store: any,
	keys: Uint8Array[],
	values: Uint8Array[],
	dataFiles: string[],
	indexFiles: string[] = [],
): Promise<BenchmarkResult> {
	console.log(`\n📊 ${name}`);

	const writeSamples: number[] = [];

	// Write benchmark with chunk sampling
	console.log("  Writing...");
	let sampleStart = performance.now();
	let sampleOps = 0;
	const totalWriteStart = performance.now();

	for (let i = 0; i < TOTAL_ENTRIES; i++) {
		await store.set(keys[i]!, values[i]!);
		sampleOps++;

		if ((i + 1) % SAMPLE_INTERVAL === 0 || i === TOTAL_ENTRIES - 1) {
			const elapsed = (performance.now() - sampleStart) / 1000;
			const ops = sampleOps / elapsed;
			writeSamples.push(ops);

			if ((i + 1) % (SAMPLE_INTERVAL * 2) === 0 || i === TOTAL_ENTRIES - 1) {
				console.log(
					`    ${(i + 1).toLocaleString().padStart(6)} entries: ${ops.toFixed(0).padStart(7)} ops/sec`,
				);
			}

			sampleStart = performance.now();
			sampleOps = 0;
		}
	}

	const totalWriteTime = (performance.now() - totalWriteStart) / 1000;
	const writeOps = TOTAL_ENTRIES / totalWriteTime;

	// Read benchmark - Single lookups
	console.log("  Reading (random, single)...");
	const readStart = performance.now();
	for (let i = 0; i < READ_SAMPLES; i++) {
		await store.get(keys[i]!);
	}
	const readOps = READ_SAMPLES / ((performance.now() - readStart) / 1000);

	// Read benchmark - Batch lookups
	if (store.getMany) {
		console.log("  Reading (batch, size=100)...");
		const batchReadStart = performance.now();
		const batchSize = 100;
		for (let i = 0; i < READ_SAMPLES; i += batchSize) {
			const batch = keys.slice(i, Math.min(i + batchSize, READ_SAMPLES));
			await store.getMany(batch);
		}
		const batchReadOps = READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
		console.log(
			`    Batch reads: ${batchReadOps.toFixed(0)} ops/sec (${
				(batchReadOps / readOps).toFixed(1)
			}x faster than single)`,
		);
	}

	await store.close();

	// Calculate file size
	let fileSize = 0;
	for (const f of [...dataFiles, ...indexFiles]) {
		try {
			const stat = await Deno.stat(f);
			fileSize += stat.size;
		} catch {}
	}

	// Individual files are kept - cleanup happens at start/end of main()

	return {
		name,
		writeOps,
		readOps,
		writeSamples,
		fileSize,
	};
}

async function benchmarkSQLite(keys: Uint8Array[], values: Uint8Array[]): Promise<BenchmarkResult> {
	console.log("\n📊 SQLite");

	try {
		await Deno.remove("data/bench_sqlite.db");
	} catch {}

	const db = new DatabaseSync("data/bench_sqlite.db");
	// Minimize SQLite caching for fair comparison with disk-based stores
	db.exec(`PRAGMA journal_mode = WAL`);
	db.exec(`PRAGMA synchronous = NORMAL`);
	db.exec(`PRAGMA cache_size = 10`); // Minimal cache (10 pages = ~40KB)
	db.exec(`CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)`);
	const insertStmt = db.prepare("INSERT INTO kv (key, value) VALUES (?, ?)");
	const selectStmt = db.prepare("SELECT value FROM kv WHERE key = ?");

	const writeSamples: number[] = [];

	console.log("  Writing...");
	let sampleStart = performance.now();
	let sampleOps = 0;
	const totalWriteStart = performance.now();

	for (let i = 0; i < TOTAL_ENTRIES; i++) {
		insertStmt.run(keys[i]!, values[i]!);
		sampleOps++;

		if ((i + 1) % SAMPLE_INTERVAL === 0 || i === TOTAL_ENTRIES - 1) {
			const elapsed = (performance.now() - sampleStart) / 1000;
			const ops = sampleOps / elapsed;
			writeSamples.push(ops);

			if ((i + 1) % (SAMPLE_INTERVAL * 2) === 0 || i === TOTAL_ENTRIES - 1) {
				console.log(
					`    ${(i + 1).toLocaleString().padStart(6)} entries: ${ops.toFixed(0).padStart(7)} ops/sec`,
				);
			}

			sampleStart = performance.now();
			sampleOps = 0;
		}
	}

	const totalWriteTime = (performance.now() - totalWriteStart) / 1000;
	const writeOps = TOTAL_ENTRIES / totalWriteTime;

	console.log("  Reading (random, single)...");
	const readStart = performance.now();
	for (let i = 0; i < READ_SAMPLES; i++) {
		selectStmt.get(keys[i]!);
	}
	const readOps = READ_SAMPLES / ((performance.now() - readStart) / 1000);

	console.log("  Reading (batch, size=100)...");
	// SQLite batch read using IN clause
	const batchReadStart = performance.now();
	const batchSize = 100;
	const placeholders = new Array(batchSize).fill("?").join(",");
	const batchStmt = db.prepare(`SELECT value FROM kv WHERE key IN (${placeholders})`);

	for (let i = 0; i < READ_SAMPLES; i += batchSize) {
		const batch = keys.slice(i, Math.min(i + batchSize, READ_SAMPLES));
		batchStmt.all(...batch);
	}
	const batchReadOps = READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(
		`    Batch reads: ${batchReadOps.toFixed(0)} ops/sec (${(batchReadOps / readOps).toFixed(1)}x vs single)`,
	);

	db.close();

	const fileStat = await Deno.stat("data/bench_sqlite.db");

	try {
		await Deno.remove("data/bench_sqlite.db");
	} catch {}

	return {
		name: "SQLite",
		writeOps,
		readOps,
		writeSamples,
		fileSize: fileStat.size,
	};
}

function formatBytes(bytes: number): string {
	if (bytes < 1024) return `${bytes}B`;
	if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
	return `${(bytes / (1024 * 1024)).toFixed(2)}MB`;
}

function printWriteConsistency(results: BenchmarkResult[]) {
	console.log("\n📈 WRITE SPEED CONSISTENCY (by 5k chunks)");
	console.log("─".repeat(80));

	const chunks = results[0]!.writeSamples.length;

	// Header
	let header = `${"Chunk".padStart(6)} │`;
	for (const r of results) {
		header += ` ${r.name.padStart(14)} │`;
	}
	console.log(header);
	console.log("─".repeat(80));

	// Data rows
	for (let i = 0; i < chunks; i++) {
		const entries = (i + 1) * SAMPLE_INTERVAL;
		let row = `${entries.toLocaleString().padStart(6)} │`;

		for (const r of results) {
			const ops = r.writeSamples[i] || 0;
			row += ` ${ops.toFixed(0).padStart(14)} │`;
		}
		console.log(row);
	}

	console.log("─".repeat(80));

	// Min/Max/StdDev
	let statsRow = `${"Min".padStart(6)} │`;
	for (const r of results) {
		const min = Math.min(...r.writeSamples);
		statsRow += ` ${min.toFixed(0).padStart(14)} │`;
	}
	console.log(statsRow);

	statsRow = `${"Max".padStart(6)} │`;
	for (const r of results) {
		const max = Math.max(...r.writeSamples);
		statsRow += ` ${max.toFixed(0).padStart(14)} │`;
	}
	console.log(statsRow);

	statsRow = `${"Avg".padStart(6)} │`;
	for (const r of results) {
		const avg = r.writeSamples.reduce((a, b) => a + b, 0) / r.writeSamples.length;
		statsRow += ` ${avg.toFixed(0).padStart(14)} │`;
	}
	console.log(statsRow);

	statsRow = `${"CV%".padStart(6)} │`;
	for (const r of results) {
		const avg = r.writeSamples.reduce((a, b) => a + b, 0) / r.writeSamples.length;
		const variance = r.writeSamples.reduce((a, b) => a + Math.pow(b - avg, 2), 0) / r.writeSamples.length;
		const cv = (Math.sqrt(variance) / avg) * 100;
		statsRow += ` ${cv.toFixed(1).padStart(14)} │`;
	}
	console.log(statsRow);

	console.log("─".repeat(80));
}

function printResults(results: BenchmarkResult[]) {
	console.log("\n" + "=".repeat(80));
	console.log("📊 RESULTS SUMMARY");
	console.log("=".repeat(80));

	// Header
	console.log(
		`\n${"Store".padEnd(15)} │ ${"Avg Writes".padStart(12)} │ ${"Reads".padStart(10)} │ ${
			"File Size".padStart(12)
		}`,
	);
	console.log("─".repeat(80));

	for (const r of results) {
		console.log(
			`${r.name.padEnd(15)} │ ${r.writeOps.toFixed(0).padStart(12)} │ ${r.readOps.toFixed(0).padStart(10)} │ ${
				formatBytes(r.fileSize).padStart(12)
			}`,
		);
	}

	console.log("\n" + "─".repeat(80));

	// Winners
	const bestWrite = results.reduce((a, b) => a.writeOps > b.writeOps ? a : b);
	const bestRead = results.reduce((a, b) => a.readOps > b.readOps ? a : b);
	const smallest = results.reduce((a, b) => a.fileSize < b.fileSize ? a : b);

	// Find most consistent (lowest coefficient of variation)
	const mostConsistent = results.reduce((a, b) => {
		const avgA = a.writeSamples.reduce((x, y) => x + y, 0) / a.writeSamples.length;
		const varA = a.writeSamples.reduce((x, y) => x + Math.pow(y - avgA, 2), 0) / a.writeSamples.length;
		const cvA = Math.sqrt(varA) / avgA;

		const avgB = b.writeSamples.reduce((x, y) => x + y, 0) / b.writeSamples.length;
		const varB = b.writeSamples.reduce((x, y) => x + Math.pow(y - avgB, 2), 0) / b.writeSamples.length;
		const cvB = Math.sqrt(varB) / avgB;

		return cvA < cvB ? a : b;
	});

	console.log("🏆 WINNERS:");
	console.log(`   Fastest Writes: ${bestWrite.name} (${bestWrite.writeOps.toFixed(0)} ops/sec)`);
	console.log(`   Fastest Reads:  ${bestRead.name} (${bestRead.readOps.toFixed(0)} ops/sec)`);
	console.log(`   Smallest File:  ${smallest.name} (${formatBytes(smallest.fileSize)})`);
	console.log(`   Most Consistent: ${mostConsistent.name}`);

	console.log("\n" + "=".repeat(80));
}

async function benchmarkRocksLite(keys: Uint8Array[], values: Uint8Array[]): Promise<BenchmarkResult> {
	const dataFile = await Deno.open("data/bench_rockslite.db", { read: true, write: true, create: true });

	const store = new RocksLite(dataFile, {
		keySize: KEY_SIZE,
		valueSize: VALUE_SIZE,
		memtableSize: 250_000, // Much larger memtable - only 4 flushes for 1M entries
		blockSize: 65536,
		compression: false,
		blockCacheSize: 5000,
	});

	await store.init();

	const result = await benchmarkStore(
		"RocksLite",
		store,
		keys,
		values,
		["data/bench_rockslite.db"],
		[],
	);

	// Print cache stats
	const stats = store.getStats();
	console.log(`    Cache hit rate: ${(stats.cacheHitRate * 100).toFixed(1)}%`);
	console.log(`    Cache entries: ${stats.cacheEntries}`);

	dataFile.close();

	return result;
}

async function main() {
	// Clean up data directory
	try {
		await Deno.remove("data", { recursive: true });
	} catch {}
	await Deno.mkdir("data", { recursive: true });

	console.log("🔥 KV Store Benchmark");
	console.log(`   Entries: ${TOTAL_ENTRIES.toLocaleString()}`);
	console.log(`   Key size: ${KEY_SIZE}B, Value size: ${VALUE_SIZE}B`);
	console.log(`   Sampling every ${SAMPLE_INTERVAL.toLocaleString()} entries`);

	// Generate test data once
	console.log("\n📦 Generating test data...");
	const keys: Uint8Array[] = [];
	const values: Uint8Array[] = [];
	for (let i = 0; i < TOTAL_ENTRIES; i++) {
		const { key, value } = generateKeyValue();
		keys.push(key);
		values.push(value);
	}

	const results: BenchmarkResult[] = [];

	results.push(await benchmarkSQLite(keys, values));
	results.push(await benchmarkRocksLite(keys, values));

	printResults(results);
	printWriteConsistency(results);

	// Clean up data directory at the end
	try {
		await Deno.remove("data", { recursive: true });
	} catch {}
}

main().catch(console.error);
