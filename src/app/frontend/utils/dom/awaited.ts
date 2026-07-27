import { ref, Sync } from "@purifyjs/core";

export function awaited<T, const U = null>(
	promise: Promise<T>,
	until?: U,
): Sync<T | U>;
export function awaited(
	promise: Promise<unknown>,
	until: unknown = null,
): Sync<unknown> {
	const state = ref(until);
	promise.then((value) => state.set(value));
	promise.catch(console.error);
	return state;
}
