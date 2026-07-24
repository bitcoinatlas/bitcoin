import { tags, toChild } from "@purifyjs/core";
import { GlobalStyle } from "~/app/frontend/style.ts";
import { ChainTimeline } from "~/app/frontend/components/ChainTimeline.ts";
import { css } from "~/app/frontend/utils/css.ts";

await import("@ungap/custom-elements");

const { body } = tags;

function App() {
	const self = body().$bind(AppStyle.useScope());

	ChainTimeline().then((timeline) => self.append$(timeline));

	return self;
}

const AppStyle = css`
	:scope {
		display: block grid;
		min-block-size: 100dvb;
	}
`;

document.adoptedStyleSheets.push(GlobalStyle.sheet());
document.body.replaceWith(toChild(App()));
