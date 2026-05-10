import { Sync } from "@purifyjs/core";

export function defer<T>(signal: Sync<T>, timeout_ms = 1000): Sync<T> {
	return new Sync<T>((set) => {
		set(signal.get());
		let lastTimeout: ReturnType<typeof setTimeout> | undefined;
		const unfollow = signal.follow((value) => {
			if (lastTimeout != null) {
				clearTimeout(lastTimeout);
			}
			lastTimeout = setTimeout(() => set(value), timeout_ms);
		});

		return () => {
			unfollow();
			if (lastTimeout != null) {
				clearTimeout(lastTimeout);
			}
		};
	});
}
