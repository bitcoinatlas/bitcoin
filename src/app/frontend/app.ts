import { tags, toChild } from "@purifyjs/core";
import { encodeHex } from "@std/encoding";
import { api } from "~/app/frontend/api.ts";
import { BlockView } from "~/app/frontend/components/BlockView.ts";
import { ChainTimeline } from "~/app/frontend/components/ChainTimeline.ts";
import { fragment } from "~/app/frontend/fragment.ts";
import { GlobalStyle } from "~/app/frontend/style.ts";
import { awaited } from "~/app/frontend/utils/dom/awaited.ts";
import { useReplaceChildren } from "~/app/frontend/utils/dom/bind.ts";
import { css } from "~/app/frontend/utils/dom/css.ts";

await import("@ungap/custom-elements");

const { body, main, header, progress } = tags;

function App() {
	const self = body().$bind(AppStyle.useScope());

	const view = fragment.derive((fragment) => {
		if (fragment.kind === "home") {
			return null;
		}
		if (fragment.kind === "block.hash") {
			const hashOrHeight = encodeHex(fragment.hash.toReversed());
			return awaited(
				api.fetch("GET /v1/block/:hashOrHeight", { params: { pathname: { hashOrHeight } } })
					.then((block) => (block ? BlockView(block) : null)),
			);
		}
		if (fragment.kind === "block.height") {
			const hashOrHeight = `${fragment.height}`;
			return awaited(
				api.fetch("GET /v1/block/:hashOrHeight", { params: { pathname: { hashOrHeight } } })
					.then((block) => (block ? BlockView(block) : null)),
			);
		}
		if (fragment.kind === "tx") {
			return null;
		}
	});

	self.append$(
		header().$bind(useReplaceChildren(awaited(ChainTimeline(), progress()))),
		main().$bind(useReplaceChildren(view)),
	);

	return self;
}

const AppStyle = css`
	:scope {
		display: block grid;
		min-block-size: 100dvb;

		grid-template-areas: "main header";
		grid-template-columns: 1fr minmax(0, 30em);
	}

	main {
		grid-area: main;
		container-type: inline-size;
		display: block grid;
		justify-items: center;
	}

	header {
		grid-area: header;
		display: block grid;
		container-type: inline-size;
	}
`;

document.adoptedStyleSheets.push(GlobalStyle.sheet());
document.body.replaceWith(toChild(App()));
