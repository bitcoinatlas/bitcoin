import { sync } from "@purifyjs/core";
import { decodeHex } from "@std/encoding";

const fragment = sync<string>((set) => {
	set(location.hash);
	const interval = setInterval(() => set(location.hash), 100);
	return () => clearInterval(interval);
}).derive((hash) => hash.slice(1) || undefined);

export type ViewFragment =
	| { kind: "home" }
	| { kind: "block.height"; height: number }
	| { kind: "block.hash"; hash: Uint8Array }
	| { kind: "tx"; txId: Uint8Array };

export const viewFragment = fragment.derive((value): ViewFragment => {
	try {
		if (!value) {
			return { kind: "home" };
		}

		const [, view, ...rest] = value.split("/");
		if (view === "block") {
			const [maybeHashOrHeight] = rest;
			if (!maybeHashOrHeight) throw null;

			if (maybeHashOrHeight.length === 32 * 2) {
				const hash = decodeHex(maybeHashOrHeight).reverse();
				return { kind: "block.hash", hash };
			}

			const height = Number(maybeHashOrHeight);
			if (!Number.isInteger(height)) throw null;
			if (height < 0) throw null;
			return { kind: "block.height", height };
		}

		if (view === "tx") {
			const [maybeTxId] = rest;
			if (!maybeTxId) throw null;

			if (maybeTxId.length === 32 * 2) {
				const txId = decodeHex(maybeTxId).reverse();
				return { kind: "tx", txId };
			}
		}

		throw null;
	} catch {
		location.replace("#");
		return { kind: "home" };
	}
});
