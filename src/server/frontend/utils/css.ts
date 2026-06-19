import type { Lifecycle } from "@purifyjs/core";

declare global {
	interface DOMStringMap {
		scope?: string | undefined;
	}
}

export function style(...params: Parameters<typeof String.raw>): Lifecycle.OnConnected {
	const raw = String.raw(...params);
	const scopeId = Math.random().toString(36).slice(2);
	const scopeRaw = `[data-scope="${scopeId}"] {${raw}}`;
	const scopeSheet = new CSSStyleSheet();
	scopeSheet.replaceSync(scopeRaw);

	return (element) => {
		document.adoptedStyleSheets.push(scopeSheet);
		element.dataset.scope = scopeId;

		return () => {
			const index = document.adoptedStyleSheets.indexOf(scopeSheet);
			if (index === -1) return;
			document.adoptedStyleSheets.splice(index, 1);
		};
	};
}

export function mixin(...params: Parameters<typeof String.raw>): string {
	return String.raw(...params);
}

export function css(...params: Parameters<typeof String.raw>) {
	return new CssTemplate(String.raw(...params));
}

export class CssTemplate {
	public readonly raw: string;

	constructor(raw: string) {
		this.raw = raw;
	}

	#sheetCache: CSSStyleSheet | undefined;
	public sheet(): CSSStyleSheet {
		if (this.#sheetCache) {
			return this.#sheetCache;
		}
		const sheet = (this.#sheetCache = new CSSStyleSheet());
		sheet.replaceSync(this.raw);
		return sheet;
	}

	#scopeId: string | undefined;
	public useScope(): Lifecycle.OnConnected {
		if (!this.#scopeId) {
			const scopeId = Math.random().toString(36).slice(2);
			const scopeRaw = `@scope ([data-scope="${scopeId}"]) to ([data-scope]) {${this.raw}}`;
			const scopeSheet = new CSSStyleSheet();
			scopeSheet.replaceSync(scopeRaw);
			document.adoptedStyleSheets.push(scopeSheet);
			this.#scopeId = scopeId;
		}

		return (element) => {
			if (element.dataset.scope === this.#scopeId) return;
			element.dataset.scope = this.#scopeId;
		};
	}
}
