import { tags } from "@purifyjs/core";
import { css } from "~/app/frontend/utils/css.ts";
import { formatBlockHeight, formatBytes, formatHash } from "~/app/frontend/utils/format.ts";
import { Block } from "~/app/routes.ts";

export function BlockCard(tip: number, block: Block) {
	const { article, dl, dt, dd, div } = tags;

	const self = article().$bind(BlockCardStyle.useScope());

	self.append$(
		dl().append$(
			div({ class: "height" }).append$(
				dt().textContent("Height"),
				dd().textContent(formatBlockHeight(block.height)),
			),
			div({ class: "hash" }).append$(
				dt().textContent("Hash"),
				dd().textContent(formatHash(block.header.hash())),
			),
			div({ class: "confirmations" }).append$(
				dt().textContent("Confirmations"),
				dd().textContent(formatBlockHeight(tip - block.height + 1)),
			),
			div({ class: "size" }).append$(
				dt().textContent("Size"),
				dd().textContent(block.size ? formatBytes(block.size) : "unknown"),
			),
		),
	);

	return self;
}

const BlockCardStyle = css`
	:scope {
		display: block grid;
	}

	dl {
		display: block grid;
	}
`;
