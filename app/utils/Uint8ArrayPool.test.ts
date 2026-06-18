import { assertEquals, assertNotStrictEquals, assertStrictEquals, assertThrows } from "@std/assert";
import { Uint8ArrayPool } from "~/utils/Uint8ArrayPool.ts";

Deno.test("Uint8ArrayPool reuses released buffers", () => {
	const pool = new Uint8ArrayPool();
	const first = pool.take(100);
	const buffer = first.buffer;
	assertEquals(first.length, 100);
	assertEquals(first.capacity, 128);

	first.release();
	assertEquals(pool.retainedBytes, 128);

	const second = pool.take(80);
	assertStrictEquals(second.buffer, buffer);
	assertEquals(second.length, 80);
	assertEquals(pool.retainedBytes, 0);
	second.release();
});

Deno.test("Uint8ArrayPool nodes are Disposable for using", () => {
	const pool = new Uint8ArrayPool();
	{
		using bytes = pool.take(9);
		bytes[0] = 123;
		assertEquals(bytes.length, 9);
		assertEquals(bytes.capacity, 16);
	}

	assertEquals(pool.retainedBytes, 16);
});

Deno.test("Uint8ArrayPool ignores duplicate releases", () => {
	const pool = new Uint8ArrayPool();
	const bytes = pool.take(4);
	bytes.release();
	bytes.release();
	assertEquals(pool.retainedBytes, 4);
});

Deno.test("Uint8ArrayPool honors max retained bytes", () => {
	const pool = new Uint8ArrayPool({ maxRetainedBytes: 8 });
	const small = pool.take(8);
	const large = pool.take(9);

	small.release();
	large.release();

	assertEquals(pool.retainedBytes, 8);
	assertStrictEquals(pool.take(8).capacity, 8);
	assertNotStrictEquals(pool.take(9).buffer, large.buffer);
});

Deno.test("Uint8ArrayPool rejects invalid lengths", () => {
	const pool = new Uint8ArrayPool();
	assertThrows(() => pool.take(-1), RangeError);
	assertThrows(() => pool.take(0.5), RangeError);
	assertThrows(() => pool.take(Number.MAX_SAFE_INTEGER + 1), RangeError);
});
