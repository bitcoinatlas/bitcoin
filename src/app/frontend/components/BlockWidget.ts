import { Sync, tags } from "@purifyjs/core";
import { encodeHex } from "@std/encoding";
import { useStyleProperty } from "~/app/frontend/utils/dom/bind.ts";
import { css } from "~/app/frontend/utils/dom/css.ts";
import { formatBlockHeight, formatBlockVersion, formatBytesDecimal, formatDifficulty } from "~/app/frontend/utils/format.ts";
import { Block } from "~/app/routes.ts";
import { MAX_BLOCK_SIZE } from "~/constants.ts";
import { difficultyFromHeader } from "~/libs/bitcoin/pow.ts";

export function BlockWidget(props: {
	block: Block;
	tipHeight: Sync<number>;
}) {
	const { block, tipHeight } = props;
	const { a, article, dl, dt, dd, div } = tags;

	const hash = block.header.hash().toReversed();

	const wireSize = block.info?.wireSize ?? 0;
	const t = wireSize / MAX_BLOCK_SIZE;
	const self = a().href(`#/block/${encodeHex(hash)}`)
		.$bind(BlockWidgetStyle.useScope())
		.$bind(useStyleProperty("--filled", `${t}`));

	const confirmations = tipHeight.derive((tip) => tip - block.height + 1);

	self.append$(
		article().append$(
			div({ class: "stripe" }).append$(
				hash.values().map((byte) => div().$bind(useStyleProperty("--hue", `${Math.round((byte / 255) * 360)}`))),
			),
			dl().append$(
				div({ class: "height" }).append$(
					dt().textContent("Height"),
					dd().textContent(formatBlockHeight(block.height)),
				),
				div({ class: "size" }).append$(
					dt().textContent("Size"),
					dd().textContent(block.info ? formatBytesDecimal(wireSize) : "unknown"),
				),
				div({ class: "difficulty" }).append$(
					dt().textContent("Difficulty"),
					dd().textContent(`${formatDifficulty(difficultyFromHeader(block.header))}`),
				),
				div({ class: "version" }).append$(
					dt().textContent("Version"),
					dd().textContent(formatBlockVersion(block.header.version)),
				),
				div({ class: "confirmations" }).append$(
					dt().textContent("Confirmations"),
					dd().textContent(confirmations.derive((n) => `${formatBlockHeight(n)} ${n === 1 ? "block" : "blocks"} ago`)),
				),
			),
		),
	);

	return self;
}

const BlockWidgetStyle = css`
	:scope {
		display: block grid;
		color: inherit;
		font: inherit;
		&:hover {
			text-decoration: none;
		}
	}

	article {
		container-type: inline-size;

		--fill: clamp(0, var(--filled, 0), 1);
		--accent: var(--accent-base);

		display: block grid;
		grid-template-columns: auto minmax(0, 1fr);
		align-items: stretch;
		gap: 1.1em;

		padding-block: 1.05em;
		padding-inline: 1.15em;
		border-radius: var(--panel-radius);

		background-image: var(--panel-surface);
		box-shadow: var(--panel-shadow);

		isolation: isolate;
		position: relative;
		overflow: clip;
		&::before {
			content: "";
			position: absolute;
			inset: 0;
			z-index: -1;
			background-image:
				linear-gradient(to left,
				transparent calc(var(--fill) * 100% - 0.14em),
				color-mix(in srgb, var(--accent) 75%, transparent) calc(var(--fill) * 100% - 0.14em),
				color-mix(in srgb, var(--accent) 75%, transparent) calc(var(--fill) * 100%),
				transparent calc(var(--fill) * 100%)),
				linear-gradient(to left,
				color-mix(in srgb, var(--accent) 22%, transparent) 0%,
				transparent calc(var(--fill) * 100%));
		}
	}

	.stripe {
		display: block grid;
		grid-auto-rows: 1fr;
		inline-size: 0.8em;
		border-radius: var(--radius-min);
		overflow: hidden;
		align-self: stretch;
	}

	.stripe > * {
		background-color: hsl(var(--hue) 55% 56%);
	}

	dl {
		display: block grid;
		grid-template-columns: auto minmax(0, 1fr) auto;
		grid-template-areas:
			"difficulty		difficulty		height"
			"version		version			."
			"confirmations	confirmations	size";
		gap: 0.25em;
		align-items: baseline;
	}

	.height {
		grid-area: height;
	}
	.size {
		grid-area: size;
	}
	.difficulty {
		grid-area: difficulty;
	}
	.version {
		grid-area: version;
	}
	.confirmations {
		grid-area: confirmations;
	}

	dl > * {
		display: block grid;
		overflow: hidden;
	}

	dt {
		font-size: 0.6em;
		letter-spacing: 0.14em;
		text-transform: uppercase;
		color: color-mix(in srgb, currentcolor 50%, transparent);
	}

	dd {
		font-variant-numeric: tabular-nums;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	/* inline end */
	.height,
	.size {
		justify-self: end;
		text-align: end;
	}

	/* no title */
	:is(.height, .confirmations) dt {
		position: absolute;
		scale: 0;
	}

	/* small value */
	:is(.difficulty, .version) dd {
		font-size: .85em;
	}

	.height {
		position: relative;
		align-self: center;
	}

	.height dd {
		font-size: 2em;
		line-height: 1;
		font-weight: bolder;
	}

	.size {
		font-weight: bolder;
	}

	.confirmations {
		align-self: end;
		font-size: .7em;
		color: color-mix(in srgb, currentcolor 72%, transparent);
	}

	@container (inline-size <= 20em) {
		dl {
			grid-template-columns: minmax(0, 1fr) auto;
			grid-template-areas:
				"size			height"
				"difficulty 	difficulty"
				"version    	version"
				"confirmations  confirmations";
			row-gap: .25em;
			column-gap: .15em;
		}

		dl > * {
			align-self: baseline;
			justify-self: start;
			text-align: start;
		}

		dd {
			font-size: .9em;
		}
	}
`;
