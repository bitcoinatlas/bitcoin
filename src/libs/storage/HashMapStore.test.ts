import { assertEquals } from "@std/assert";
import { Codec, U32, VarInt } from "@nomadshiba/codec";
import { Bytes32 } from "~/codec/primitives/Bytes32.ts";
import { U48 } from "~/codec/primitives/U48.ts";
import { HashMapStore, HashMapStoreOptions } from "~/libs/storage/HashMapStore.ts";

function tmpDir(): string {
	return Deno.makeTempDirSync({ prefix: "hashmap_test_" });
}

function key32(n: number): Uint8Array {
	const b = new Uint8Array(32);
	new DataView(b.buffer).setUint32(0, n);
	// vary the tail too so distinct n never collide byte-wise
	new DataView(b.buffer).setUint32(28, n * 2654435761 >>> 0);
	return b;
}

type Opts<V extends Codec> = Partial<HashMapStoreOptions<typeof U48, typeof Bytes32, V>>;
function options<V extends Codec>(
	path: string,
	value: V,
	overrides?: Opts<V>,
): HashMapStoreOptions<typeof U48, typeof Bytes32, V> {
	return {
		path,
		pointer: U48,
		key: Bytes32,
		value,
		writable: true,
		targetRatio: 0.25, // ~4 entries per bucket
		maxRatioDrift: 0.5,
		entries: { maxChunkSize: 1 * 1024 * 1024 },
		buckets: { itemsPerChunk: 1_000_000 },
		...overrides,
	};
}

Deno.test("set/get roundtrip + duplicate rejection", () => {
	using map = HashMapStore.open(options(tmpDir(), U32));

	for (let i = 0; i < 100; i++) {
		assertEquals(map.set(key32(i), i * 7), true);
	}
	// duplicate
	assertEquals(map.set(key32(42), 999), false);
	assertEquals(map.get(key32(42)), 42 * 7);

	for (let i = 0; i < 100; i++) {
		assertEquals(map.get(key32(i)), i * 7);
	}
	assertEquals(map.get(key32(1000)), undefined);
});

Deno.test("rehash on drift keeps all entries findable", () => {
	using map = HashMapStore.open(options(tmpDir(), U32, { targetRatio: 0.5, maxRatioDrift: 0.25 }));

	const N = 500;
	for (let i = 0; i < N; i++) map.set(key32(i), i);
	for (let i = 0; i < N; i++) assertEquals(map.get(key32(i)), i, `missing ${i}`);
});

Deno.test("truncate drops suffix, survivors stay findable", () => {
	const path = tmpDir();
	const open = () => HashMapStore.open(options(path, U32));

	const map = open();
	for (let i = 0; i < 30; i++) map.set(key32(i), i);
	const cut = map.size();
	for (let i = 30; i < 50; i++) map.set(key32(i), i);

	map.truncate(cut);

	for (let i = 0; i < 30; i++) assertEquals(map.get(key32(i)), i, `survivor ${i} missing`);
	for (let i = 30; i < 50; i++) assertEquals(map.get(key32(i)), undefined, `dropped ${i} still present`);
	// can insert again after truncate
	assertEquals(map.set(key32(30), 12345), true);
	assertEquals(map.get(key32(30)), 12345);
	map.close();
});

Deno.test("async get matches sync get", async () => {
	using map = HashMapStore.open(options(tmpDir(), U32, { targetRatio: 0.33, maxRatioDrift: 0.5 }));
	for (let i = 0; i < 120; i++) map.set(key32(i), i * 3);
	for (let i = 0; i < 120; i++) {
		assertEquals(await map.getAsync(key32(i)), i * 3, `async ${i}`);
	}
	assertEquals(await map.getAsync(key32(9999)), undefined);
});

Deno.test("rehash across compressed entries (writeInto inflate path)", () => {
	using map = HashMapStore.open(options(tmpDir(), U32, {
		targetRatio: 0.5,
		maxRatioDrift: 0.25,
		entries: {
			maxChunkSize: 4 * 1024,
			compression: {
				maxInflatedChunkAge: 60_000,
				maxInflatedChunks: 4,
				zstd: { compress: { compressionLevel: 3 }, decompress: {} },
			},
		},
	}));
	const N = 300;
	for (let i = 0; i < N; i++) map.set(key32(i), i);
	// force an explicit rehash to exercise inline-link patching
	map.rehash();
	for (let i = 0; i < N; i++) assertEquals(map.get(key32(i)), i, `compressed ${i}`);
});

Deno.test("reopen recovers heads (stale rebuild path)", () => {
	const path = tmpDir();
	{
		using map = HashMapStore.open(options(path, VarInt));
		for (let i = 0; i < 60; i++) map.set(key32(i), i);
	}
	// Force a stale rebuild on next open.
	Deno.writeFileSync(`${path}/meta`, new Uint8Array([1])); // stale = true

	using map = HashMapStore.open(options(path, VarInt));
	for (let i = 0; i < 60; i++) assertEquals(map.get(key32(i)), i, `post-recovery ${i} missing`);
});
