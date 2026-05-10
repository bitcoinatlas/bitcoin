import { computed, Sync } from "@purifyjs/core";

type SyncUnroll_<T, SyncExcluded = Exclude<T, Sync<any>>> = [SyncExcluded] extends [never]
	? T extends Sync<infer U> ? SyncUnroll_<U> : T
	: SyncExcluded;
export type SyncUnroll<T> = SyncUnroll_<T>;

export function unroll<T>(signal: T): Sync<SyncUnroll<T>>;
export function unroll(signal: Sync<unknown>) {
	return computed(() => {
		let value: unknown = signal;
		while (value instanceof Sync) {
			value = value.get();
		}
		return value;
	});
}
