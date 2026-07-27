import { Sync, sync } from "@purifyjs/core";
import { _ } from "~/types.ts";

type SyncUnroll_<T, SyncExcluded = Exclude<T, Sync<_>>> = [SyncExcluded] extends [never] ? T extends Sync<infer U> ? SyncUnroll_<U> : T
	: SyncExcluded;
export type SyncUnroll<T> = SyncUnroll_<T>;

export function unroll<T>(signal: T): Sync<SyncUnroll<T>>;
export function unroll(signal: Sync<unknown>) {
	return sync<unknown>((set) => {
		let innerUnfollow: Sync.Unfollower | null = null;

		const follow = (value: unknown) => {
			innerUnfollow?.();
			innerUnfollow = null;

			if (value instanceof Sync) {
				innerUnfollow = value.follow(follow, true);
			} else {
				set(value);
			}
		};

		const outerUnfollow = signal.follow(follow, true);

		return () => {
			innerUnfollow?.();
			outerUnfollow();
		};
	});
}
