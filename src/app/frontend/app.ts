import { tags, toChild } from "@purifyjs/core";
import { GlobalStyle } from "~/app/frontend/style.ts";
import { ChainTimeline } from "~/app/frontend/components/ChainTimeline.ts";
import { css } from "~/app/frontend/utils/css.ts";
import { useReplaceChildren } from "~/app/frontend/utils/bind.ts";
import { awaited } from "~/app/frontend/utils/awaited.ts";

await import("@ungap/custom-elements");

const { body, main, header, progress } = tags;

function App() {
	const self = body().$bind(AppStyle.useScope());

	self.append$(
		header().$bind(useReplaceChildren(awaited(ChainTimeline(), progress()))),
		main(),
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
	}

	header {
		grid-area: header;
		display: block grid;
	}
`;

document.adoptedStyleSheets.push(GlobalStyle.sheet());
document.body.replaceWith(toChild(App()));
