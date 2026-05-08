import { BytesCodec } from "@nomadshiba/codec";
import { DatabaseSync } from "node:sqlite";
import { createArrayStore } from "~/lib/storage/ArrayStore.ts";
import { createBlobStore } from "~/lib/storage/BlobStore.ts";
import { createKVStore } from "~/lib/storage/KVStore.ts";

// ─── KVStore constants ────────────────────────────────────────────────────────
const KEY_SIZE = 32;
const VALUE_SIZE = 128;
const KV_TOTAL = 1_000_000;
const KV_READ_SAMPLES = 10_000;
const KV_BATCH_SIZE = 100_000;

// ─── ArrayStore constants ─────────────────────────────────────────────────────
const ARRAY_ITEM_SIZE = 64;
const ARRAY_TOTAL = 1_000_000;
const ARRAY_READ_SAMPLES = 10_000;
const ARRAY_RANGE_SIZE = 100;
const ARRAY_BATCH_SIZE = 100_000;

// ─── BlobStore constants ──────────────────────────────────────────────────────
const BLOB_SIZE = 512;
const BLOB_TOTAL = 100_000;
const BLOB_READ_SAMPLES = 5_000;

// ─── Helpers ──────────────────────────────────────────────────────────────────

function randomBytes(size: number): Uint8Array {
	const buf = new Uint8Array(size);
	crypto.getRandomValues(buf);
	return buf;
}

// ─── KVStore ─────────────────────────────────────────────────────────────────

async function benchmarkKVStore(keys: Uint8Array[], values: Uint8Array[]) {
	console.log("\n  KVStore");

	const store = await createKVStore({
		name: "bench_kv",
		path: "data/bench_kv",
		keyCodec: new BytesCodec({ size: KEY_SIZE }),
		valueCodec: new BytesCodec({ size: VALUE_SIZE }),
	});

	// Writes — batch tx → WAL save → WAL apply → WAL discard
	console.log("    Writing...");
	const writeStart = performance.now();
	for (let i = 0; i < KV_TOTAL; i += KV_BATCH_SIZE) {
		const end = Math.min(i + KV_BATCH_SIZE, KV_TOTAL);
		const tx = store.transaction();
		for (let j = i; j < end; j++) tx.set(keys[j]!, values[j]!);
		tx.apply();
		const wal = await store.WAL();
		await wal.save();
		await wal.apply();
		await wal.discard();

		const elapsed = (performance.now() - writeStart) / 1000;
		const rate = end / elapsed;
		console.log(`      ${(end / KV_TOTAL * 100).toFixed(1)}% - ${end.toLocaleString()} entries (${rate.toFixed(0)} ops/sec)`);
	}
	const writeOps = KV_TOTAL / ((performance.now() - writeStart) / 1000);
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	for (let i = 0; i < KV_READ_SAMPLES; i++) await store.get(keys[i]!);
	const readOps = KV_READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`      ${readOps.toFixed(0)} ops/sec`);

	// Reads (batch)
	console.log("    Reading (batch)...");
	const batchReadStart = performance.now();
	for (let i = 0; i < KV_READ_SAMPLES; i += 100) {
		await store.getMany(keys.slice(i, Math.min(i + 100, KV_READ_SAMPLES)));
	}
	const batchReadOps = KV_READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(`      ${batchReadOps.toFixed(0)} ops/sec`);

	store.close();

	let fileSize = 0;
	for await (const e of Deno.readDir("data/bench_kv")) {
		if (e.isFile) fileSize += (await Deno.stat(`data/bench_kv/${e.name}`)).size;
	}
	console.log(`      File: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);

	return { name: "KVStore", writeOps, readOps, batchReadOps, fileSize };
}

// ─── SQLite ───────────────────────────────────────────────────────────────────

async function benchmarkSQLite(keys: Uint8Array[], values: Uint8Array[]) {
	console.log("\n  SQLite");
	const db = new DatabaseSync("data/bench_sqlite.db");
	db.exec(`PRAGMA journal_mode = WAL`);
	db.exec(`PRAGMA synchronous = NORMAL`);
	db.exec(`CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)`);

	// Writes
	console.log("    Writing...");
	const writeStart = performance.now();
	for (let i = 0; i < KV_TOTAL; i += KV_BATCH_SIZE) {
		const end = Math.min(i + KV_BATCH_SIZE, KV_TOTAL);
		db.exec("BEGIN");
		const stmt = db.prepare("INSERT INTO kv (key, value) VALUES (?, ?)");
		for (let j = i; j < end; j++) stmt.run(keys[j]!, values[j]!);
		db.exec("COMMIT");

		const elapsed = (performance.now() - writeStart) / 1000;
		const rate = end / elapsed;
		console.log(`      ${(end / KV_TOTAL * 100).toFixed(1)}% - ${end.toLocaleString()} entries (${rate.toFixed(0)} ops/sec)`);
	}
	const writeOps = KV_TOTAL / ((performance.now() - writeStart) / 1000);
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	const selectStmt = db.prepare("SELECT value FROM kv WHERE key = ?");
	for (let i = 0; i < KV_READ_SAMPLES; i++) selectStmt.get(keys[i]!);
	const readOps = KV_READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`      ${readOps.toFixed(0)} ops/sec`);

	// Reads (batch)
	console.log("    Reading (batch)...");
	const batchReadStart = performance.now();
	const placeholders = new Array(100).fill("?").join(",");
	const batchStmt = db.prepare(`SELECT value FROM kv WHERE key IN (${placeholders})`);
	for (let i = 0; i < KV_READ_SAMPLES; i += 100) {
		batchStmt.all(...keys.slice(i, Math.min(i + 100, KV_READ_SAMPLES)));
	}
	const batchReadOps = KV_READ_SAMPLES / ((performance.now() - batchReadStart) / 1000);
	console.log(`      ${batchReadOps.toFixed(0)} ops/sec`);

	db.close();

	const stat = await Deno.stat("data/bench_sqlite.db");
	console.log(`      File: ${(stat.size / 1024 / 1024).toFixed(2)} MB`);

	return { name: "SQLite", writeOps, readOps, batchReadOps, fileSize: stat.size };
}

// ─── ArrayStore ───────────────────────────────────────────────────────────────

async function benchmarkArrayStore(items: Uint8Array[]) {
	console.log("\n  ArrayStore");

	const store = await createArrayStore({
		name: "bench_array",
		path: "data/bench_array",
		codec: new BytesCodec({ size: ARRAY_ITEM_SIZE }),
	});

	// Writes
	console.log("    Writing...");
	const writeStart = performance.now();
	for (let i = 0; i < ARRAY_TOTAL; i += ARRAY_BATCH_SIZE) {
		const end = Math.min(i + ARRAY_BATCH_SIZE, ARRAY_TOTAL);
		const tx = store.transaction();
		for (let j = i; j < end; j++) tx.append(items[j]!);
		tx.apply();
		const wal = await store.WAL();
		await wal.save();
		await wal.apply();
		await wal.discard();

		const elapsed = (performance.now() - writeStart) / 1000;
		const rate = end / elapsed;
		console.log(`      ${(end / ARRAY_TOTAL * 100).toFixed(1)}% - ${end.toLocaleString()} items (${rate.toFixed(0)} ops/sec)`);
	}
	const writeOps = ARRAY_TOTAL / ((performance.now() - writeStart) / 1000);
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	for (let i = 0; i < ARRAY_READ_SAMPLES; i++) await store.get(i);
	const readOps = ARRAY_READ_SAMPLES / ((performance.now() - readStart) / 1000);
	console.log(`      ${readOps.toFixed(0)} ops/sec`);

	// Reads (sequential range)
	console.log(`    Reading (range/${ARRAY_RANGE_SIZE})...`);
	const rangeIndices = new Array<number>(ARRAY_RANGE_SIZE);
	const rangeStart = performance.now();
	for (let i = 0; i < ARRAY_READ_SAMPLES; i += ARRAY_RANGE_SIZE) {
		const end = Math.min(i + ARRAY_RANGE_SIZE, ARRAY_READ_SAMPLES);
		for (let k = 0; k < end - i; k++) rangeIndices[k] = i + k;
		await store.getMany(rangeIndices.slice(0, end - i));
	}
	const rangeOps = ARRAY_READ_SAMPLES / ((performance.now() - rangeStart) / 1000);
	console.log(`      ${rangeOps.toFixed(0)} ops/sec`);

	store.close();

	let fileSize = 0;
	for await (const e of Deno.readDir("data/bench_array")) {
		if (e.isFile) fileSize += (await Deno.stat(`data/bench_array/${e.name}`)).size;
	}
	console.log(`      File: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);

	return { name: "ArrayStore", writeOps, readOps, rangeOps, fileSize };
}

// ─── BlobStore ────────────────────────────────────────────────────────────────

async function benchmarkBlobStore(blobs: Uint8Array[]) {
	console.log("\n  BlobStore");

	const store = await createBlobStore({
		name: "bench_blob",
		path: "data/bench_blob",
	});

	// Writes — one transaction for all blobs, record pointers
	console.log("    Writing...");
	const pointers: number[] = [];
	const writeStart = performance.now();
	const tx = store.transaction();
	for (let i = 0; i < BLOB_TOTAL; i++) pointers.push(tx.append(blobs[i]!));
	tx.apply();
	const wal = await store.WAL();
	await wal.save();
	await wal.apply();
	await wal.discard();
	const writeElapsed = (performance.now() - writeStart) / 1000;
	const writeOps = BLOB_TOTAL / writeElapsed;
	const writeMBps = (BLOB_TOTAL * BLOB_SIZE) / 1024 / 1024 / writeElapsed;
	console.log(`      Total: ${writeOps.toFixed(0)} ops/sec  (${writeMBps.toFixed(1)} MB/s)`);

	// Reads (single)
	console.log("    Reading (single)...");
	const readStart = performance.now();
	for (let i = 0; i < BLOB_READ_SAMPLES; i++) await store.get(pointers[i]!, BLOB_SIZE);
	const readElapsed = (performance.now() - readStart) / 1000;
	const readOps = BLOB_READ_SAMPLES / readElapsed;
	const readMBps = (BLOB_READ_SAMPLES * BLOB_SIZE) / 1024 / 1024 / readElapsed;
	console.log(`      ${readOps.toFixed(0)} ops/sec  (${readMBps.toFixed(1)} MB/s)`);

	let fileSize = 0;
	for await (const e of Deno.readDir("data/bench_blob")) {
		if (e.isFile && e.name.startsWith("chunk_")) {
			fileSize += (await Deno.stat(`data/bench_blob/${e.name}`)).size;
		}
	}
	console.log(`      File: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);

	return { name: "BlobStore", writeOps, writeMBps, readOps, readMBps, fileSize };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
	await Deno.remove("data", { recursive: true }).catch(() => {});
	await Deno.mkdir("data", { recursive: true });

	// ── KV benchmarks ──────────────────────────────────────────────────────────
	console.log(`KV Store Benchmark`);
	console.log(`  Entries: ${KV_TOTAL.toLocaleString()}, Key: ${KEY_SIZE}B, Value: ${VALUE_SIZE}B`);
	console.log(`\n  Generating KV test data...`);
	const keys: Uint8Array[] = [];
	const values: Uint8Array[] = [];
	for (let i = 0; i < KV_TOTAL; i++) {
		keys.push(randomBytes(KEY_SIZE));
		values.push(randomBytes(VALUE_SIZE));
	}

	const kvResults = [
		await benchmarkKVStore(keys, values),
		await benchmarkSQLite(keys, values),
	];

	console.log("\n" + "=".repeat(62));
	console.log("KV RESULTS");
	console.log("=".repeat(62));
	console.log("Store              Writes      Single       Batch      Size");
	console.log("-".repeat(62));
	for (const r of kvResults) {
		console.log(
			`${r.name.padEnd(17)} ${r.writeOps.toFixed(0).padStart(9)} ${r.readOps.toFixed(0).padStart(11)} ${
				r.batchReadOps.toFixed(0).padStart(11)
			} ${(r.fileSize / 1024 / 1024).toFixed(1).padStart(6)}MB`,
		);
	}

	// ── ArrayStore benchmark ───────────────────────────────────────────────────
	console.log(`\nArrayStore Benchmark`);
	console.log(`  Items: ${ARRAY_TOTAL.toLocaleString()}, Item size: ${ARRAY_ITEM_SIZE}B`);
	console.log(`\n  Generating array test data...`);
	const arrayItems: Uint8Array[] = [];
	for (let i = 0; i < ARRAY_TOTAL; i++) arrayItems.push(randomBytes(ARRAY_ITEM_SIZE));

	const arrayResult = await benchmarkArrayStore(arrayItems);

	console.log("\n" + "=".repeat(62));
	console.log("ARRAY RESULTS");
	console.log("=".repeat(62));
	console.log("Store              Writes      Single       Range      Size");
	console.log("-".repeat(62));
	console.log(
		`${arrayResult.name.padEnd(17)} ${arrayResult.writeOps.toFixed(0).padStart(9)} ${
			arrayResult.readOps.toFixed(0).padStart(11)
		} ${arrayResult.rangeOps.toFixed(0).padStart(11)} ${(arrayResult.fileSize / 1024 / 1024).toFixed(1).padStart(6)}MB`,
	);

	// ── BlobStore benchmark ────────────────────────────────────────────────────
	console.log(`\nBlobStore Benchmark`);
	console.log(`  Blobs: ${BLOB_TOTAL.toLocaleString()}, Blob size: ${BLOB_SIZE}B`);
	console.log(`\n  Generating blob test data...`);
	const blobItems: Uint8Array[] = [];
	for (let i = 0; i < BLOB_TOTAL; i++) blobItems.push(randomBytes(BLOB_SIZE));

	const blobResult = await benchmarkBlobStore(blobItems);

	console.log("\n" + "=".repeat(65));
	console.log("BLOB RESULTS");
	console.log("=".repeat(65));
	console.log("Store              Writes    Write MB/s    Single  Read MB/s  Size");
	console.log("-".repeat(65));
	console.log(
		`${blobResult.name.padEnd(17)} ${blobResult.writeOps.toFixed(0).padStart(9)} ${
			blobResult.writeMBps.toFixed(1).padStart(11)
		} ${blobResult.readOps.toFixed(0).padStart(9)} ${blobResult.readMBps.toFixed(1).padStart(9)} ${
			(blobResult.fileSize / 1024 / 1024).toFixed(1).padStart(6)}MB`,
	);

	await Deno.remove("data", { recursive: true }).catch(() => {});
}

main().catch(console.error);
