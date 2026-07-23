import { tags, toChild } from "@purifyjs/core";
import { GlobalStyle } from "~/app/frontend/style.ts";

await import("@ungap/custom-elements");

const { body } = tags;

function App() {
	const self = body();

	self.textContent("Hello");

	return self;
}

document.adoptedStyleSheets.push(GlobalStyle.sheet());

document.body.replaceWith(toChild(App()));
