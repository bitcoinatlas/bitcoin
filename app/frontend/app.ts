import { api } from "~/frontend/api.ts";
import appCss from "./app.css" with { type: "text" };
import { tags, toChild } from "@purifyjs/core";
import { sha256 } from "@noble/hashes/sha2";
import { bytesToHex } from "@noble/hashes/utils";

const appSheet = new CSSStyleSheet();
appSheet.replaceSync(appCss);
document.adoptedStyleSheets.push(appSheet);

export function App() {
	const { body, div } = tags;
	const self = body();

	const tipPromise = api.fetch("GET /v1/block/tip", {});
	const blocksPromise = tipPromise.then((tip) => api.fetch("GET /v1/block", { search: { to: tip?.height ?? 0 } }));

	blocksPromise.then((blocks) => {
		self.append$(blocks.map((block) => {
			return div().textContent(bytesToHex(block.header.hash.toReversed()));
		}));
	});

	return self;
}

document.body.replaceWith(toChild(App()));
