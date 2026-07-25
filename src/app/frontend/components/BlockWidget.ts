import { tags } from "@purifyjs/core";
import { useStyleProperty } from "~/app/frontend/utils/bind.ts";
import { css } from "~/app/frontend/utils/css.ts";
import { getRelativeDate } from "~/app/frontend/utils/date.ts";
import { formatBlockHeight, formatBytes } from "~/app/frontend/utils/format.ts";
import { Block } from "~/app/routes.ts";
import { MAX_BLOCK_SIZE } from "~/constants.ts";

export function BlockWidget(block: Block) {
	const { article, dl, dt, dd, div } = tags;

	const self = article().$bind(BlockWidgetStyle.useScope());
	// TODO: maybe make this based on weight
	self.$bind(useStyleProperty("--filled", `${(block.size ?? 0) / MAX_BLOCK_SIZE}`));

	self.append$(
		div({ class: "stripe" }).append$(
			Array.from(block.header.hash()).map((byte) => div().$bind(useStyleProperty("--hue", `${Math.round((byte / 255) * 360)}`))),
		),
		dl().append$(
			div({ class: "height" }).append$(
				dt().textContent("Height"),
				dd().textContent(formatBlockHeight(block.height)),
			),
			div({ class: "size" }).append$(
				dt().textContent("Size"),
				dd().textContent(block.size ? formatBytes(block.size) : "unknown"),
			),
			// TODO: maybe remove this in favor of "n blocks ago"
			div({ class: "timestamp" }).append$(
				dt().textContent("Timestamp"),
				dd().textContent(getRelativeDate(new Date(block.header.timestamp * 1000))),
			),
			div({ class: "bits" }).append$(
				dt().textContent("Bits"),
				dd().textContent(`${block.header.bits}`),
			),
			div({ class: "nonce" }).append$(
				dt().textContent("Nonce"),
				dd().textContent(`${block.header.nonce}`),
			),
		),
	);

	return self;
}

const BlockWidgetStyle = css`
	:scope {
		container-type: inline-size;

		--fill: clamp(0, var(--filled, 0), 1);
		--accent: var(--accent-base);

		display: block grid;
		grid-template-columns: auto minmax(0, 1fr);
		align-items: stretch;
		gap: 1.1em;

		padding-block: 1.05em;
		padding-inline: 1.15em;
		border-radius: 0.85em;

		background-color: color-mix(in srgb, var(--base), currentcolor 2%);
		background-image:
			linear-gradient(to left,
			transparent calc(var(--fill) * 100% - 0.14em),
			color-mix(in srgb, var(--accent) 75%, transparent) calc(var(--fill) * 100% - 0.14em),
			color-mix(in srgb, var(--accent) 75%, transparent) calc(var(--fill) * 100%),
			transparent calc(var(--fill) * 100%)),
			linear-gradient(to left,
			color-mix(in srgb, var(--accent) 22%, transparent) 0%,
			transparent calc(var(--fill) * 100%));

		box-shadow:
			inset 0 0.06em 0 0 color-mix(in srgb, currentcolor 9%, transparent),
			inset 0 -0.14em 10px -0.25em color-mix(in srgb, black 45%, transparent);
	}

	.stripe {
		display: block grid;
		grid-auto-rows: 1fr;
		inline-size: 0.8em;
		border-radius: 0.3em;
		overflow: hidden;
		align-self: stretch;
	}

	.stripe > * {
		background-color: hsl(var(--hue) 55% 56%);
	}

	dl {
		display: block grid;
		grid-template-columns: auto auto minmax(0, 1fr) auto;
		grid-template-areas:
			"size		.			.	height"
			".			.      		.	bits"
			"timestamp	timestamp 	.	nonce";
		gap: 0.25em;
		align-items: baseline;
		margin-block: 0;
		margin-inline: 0;
	}

	dl > * {
		display: block grid;
		overflow: hidden;
	}

	dt {
		margin-block: 0;
		margin-inline: 0;
		font-size: 0.6em;
		letter-spacing: 0.14em;
		text-transform: uppercase;
		color: color-mix(in srgb, currentcolor 50%, transparent);
	}

	dd {
		margin-block: 0;
		margin-inline: 0;
		font-variant-numeric: tabular-nums;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.confirmations {
		grid-area: confirmations;
	}

	.height,
	.bits,
	.nonce {
		justify-self: end;
		text-align: end;
	}

	:is(.height, .timestamp, .hash) dt {
		position: absolute;
		clip-path: inset(50%);
		overflow: hidden;
		white-space: nowrap;
	}

	.height {
		grid-area: height;
		position: relative;
		align-self: center;
	}

	.height dd {
		font-size: 2em;
		line-height: 1;
		font-weight: 600;
	}

	.hash {
		grid-area: hash;
	}

	.hash dd {
		font-family: var(--font-mono, ui-monospace, monospace);
		font-size: 0.95em;
		color: color-mix(in srgb, currentcolor 72%, transparent);

		display: block grid;
		grid-template-columns: 1fr auto;

		span:first-child {
			white-space: nowrap;
			overflow: hidden;
			text-overflow: ellipsis;
		}
	}

	.size {
		grid-area: size;
	}

	.timestamp {
		grid-area: timestamp;
		align-self: end;
		font-size: .7em;
		color: color-mix(in srgb, currentcolor 72%, transparent);
	}

	.bits {
		grid-area: bits;
	}

	.nonce {
		grid-area: nonce;
	}

	@container (inline-size <= 20em) {
		dl {
			grid-template-columns: minmax(0, 1fr) auto;
			grid-template-areas:
				"size		height"
				"bits       bits"
				"nonce      nonce"
				"timestamp  timestamp";
			row-gap: .25em;
			column-gap: .15em;
		}

		dl > * {
			align-self: baseline;
		}

		.bits,
		.nonce {
			justify-self: start;
			text-align: start;
		}

		dd {
			font-size: .9em;
		}
	}
`;
