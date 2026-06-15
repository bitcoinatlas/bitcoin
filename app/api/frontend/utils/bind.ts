import { type Lifecycle, type Member, Sync, toChild } from "@purifyjs/core";
import type { SyncOrValue } from "./types.ts";

export function useReplaceChildren<T extends Member>(signal: Sync<T>): Lifecycle.OnConnected {
	return (element) => signal.follow((member) => element.replaceChildren(toChild(member)), true);
}

export function useTextContent(value: Sync.Ref<string>): Lifecycle.OnConnected<HTMLTextAreaElement> {
	return (element) => {
		const abortController = new AbortController();
		element.addEventListener("input", () => value.set(element.textContent ?? ""), {
			signal: abortController.signal,
		});
		const unfollow = value.follow((value) => element.textContent = value, true);

		return () => {
			abortController.abort();
			unfollow();
		};
	};
}

export function useChecked(value: Sync.Ref<boolean>): Lifecycle.OnConnected<HTMLInputElement> {
	return (element) => {
		const abortController = new AbortController();
		element.addEventListener("change", () => value.set(element.checked), { signal: abortController.signal });
		const unfollow = value.follow((value) => element.checked = value, true);

		return () => {
			abortController.abort();
			unfollow();
		};
	};
}

export function useValue(value: Sync.Ref<string>): Lifecycle.OnConnected<HTMLInputElement | HTMLSelectElement> {
	return (element) => {
		const abortController = new AbortController();
		element.addEventListener("input", () => value.set(element.value), { signal: abortController.signal });
		const unfollow = value.follow((value) => element.value = value, true);

		return () => {
			abortController.abort();
			unfollow();
		};
	};
}

export function useSelectedValue(value: Sync.Ref<string>): Lifecycle.OnConnected<HTMLSelectElement> {
	return (element) => {
		const abortController = new AbortController();
		element.addEventListener("change", () => value.set(element.value), { signal: abortController.signal });
		const unfollow = value.follow((value) => element.value = value, true);

		return () => {
			abortController.abort();
			unfollow();
		};
	};
}

export function useValueAsNumber(value: Sync.Ref<number>): Lifecycle.OnConnected<HTMLInputElement> {
	return (element) => {
		const abortController = new AbortController();
		element.addEventListener("input", () => value.set(element.valueAsNumber), { signal: abortController.signal });
		const unfollow = value.follow((value) => element.valueAsNumber = value, true);

		return () => {
			abortController.abort();
			unfollow();
		};
	};
}

export function useValueAsDate(value: Sync.Ref<Date | null>): Lifecycle.OnConnected<HTMLInputElement> {
	return (element) => {
		const abortController = new AbortController();
		element.addEventListener("input", () => value.set(element.valueAsDate), { signal: abortController.signal });
		const unfollow = value.follow((value) => element.valueAsDate = value, true);

		return () => {
			abortController.abort();
			unfollow();
		};
	};
}

export function useClassToggle(
	classes: Record<string, SyncOrValue<boolean>>,
): Lifecycle.OnConnected<HTMLElement> {
	const unfollows: Sync.Unfollower[] = [];

	return (element) => {
		for (const className in classes) {
			const signalOrValue = classes[className];
			if (signalOrValue instanceof Sync) {
				unfollows.push(signalOrValue.follow((active) => {
					element.classList.toggle(className, active);
				}, true));
			} else {
				element.classList.toggle(className, signalOrValue);
			}
		}

		return () => {
			for (const unfollow of unfollows) {
				unfollow();
			}
			unfollows.length = 0;
		};
	};
}

export function useCssVar(
	name: string,
	value: SyncOrValue<string>,
): Lifecycle.OnConnected<HTMLElement> {
	if (value instanceof Sync) {
		return (element) =>
			value.follow((val) => {
				element.style.setProperty(`--${name}`, val);
			}, true);
	} else {
		return (element) => {
			element.style.setProperty(`--${name}`, value);
		};
	}
}

export function useAttribute(
	name: string,
	value: SyncOrValue<string | null>,
): Lifecycle.OnConnected<HTMLElement> {
	if (value instanceof Sync) {
		return (element) =>
			value.follow((val) => {
				if (val == null) {
					element.removeAttribute(name);
				} else {
					element.setAttribute(name, val);
				}
			}, true);
	} else {
		return (element) => {
			if (value == null) {
				element.removeAttribute(name);
			} else {
				element.setAttribute(name, value);
			}
		};
	}
}
