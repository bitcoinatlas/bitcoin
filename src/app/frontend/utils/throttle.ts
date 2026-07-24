import { Sync } from "@purifyjs/core";
import { SECOND } from "~/constants.ts";

export function throttle<T>(signal: Sync<T>, timeout = 1 * SECOND): Sync<T> {
	return new Sync<T>((set) => {
		set(signal.get());
		let lastTimeout: ReturnType<typeof setTimeout> | undefined | null;
		let lastValue: T;
		let hasPending = false;

		// Leading-edge: the first update after a quiet period emits immediately
		// and arms a cooldown. Updates during cooldown are held; the latest one
		// is flushed at the end (trailing), which re-arms the cooldown — so a
		// steady stream emits at most once per `timeout`, while a single update
		// with no followers emits instantly and incurs no trailing duplicate.
		const startCooldown = () => {
			lastTimeout = setTimeout(() => {
				lastTimeout = null;
				if (!hasPending) return;
				hasPending = false;
				set(lastValue);
				startCooldown();
			}, timeout);
		};

		const unfollow = signal.follow((value) => {
			if (lastTimeout != null) {
				lastValue = value;
				hasPending = true;
				return;
			}
			set(value);
			startCooldown();
		});

		return () => {
			unfollow();
			if (lastTimeout != null) clearTimeout(lastTimeout);
		};
	});
}
