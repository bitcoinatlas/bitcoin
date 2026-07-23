import { tags } from "@purifyjs/core";
import { formatBlockHeight, formatHash } from "~/app/frontend/utils/format.ts";
import { Schema } from "~/app/libs/routing/Router.ts";
import { RoutesSchema } from "~/app/routes.ts";
import { css } from "~/app/frontend/utils/css.ts";

export function BlockCard(tip: number, block: Schema.InferResult<RoutesSchema, "GET /v1/block">[number]) {
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
