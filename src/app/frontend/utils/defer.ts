import { Sync } from "@purifyjs/core";
import { SECOND } from "~/constants.ts";

export function defer<T>(signal: Sync<T>, timeout = 1 * SECOND): Sync<T> {
	return new Sync<T>((set) => {
		set(signal.get());
		let lastTimeout: ReturnType<typeof setTimeout> | undefined;
		const unfollow = signal.follow((value) => {
			if (lastTimeout != null) {
				clearTimeout(lastTimeout);
			}
			lastTimeout = setTimeout(() => set(value), timeout);
		});

		return () => {
			unfollow();
			if (lastTimeout != null) {
				clearTimeout(lastTimeout);
			}
		};
	});
}
