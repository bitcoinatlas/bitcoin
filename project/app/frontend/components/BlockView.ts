import { tags } from "@purifyjs/core";
import { encodeHex } from "@std/encoding";
import { css } from "~/frontend/utils/dom/css.ts";
import {
	formatBitcoin,
	formatBlockHeight,
	formatBlockVersion,
	formatBytesDecimal,
	formatDateTime,
	formatDifficulty,
	formatHash,
} from "~/frontend/utils/format.ts";
import { Block } from "~/routes.ts";
import { SECOND } from "@project/utils";
import { difficultyFromHeader } from "@project/bitcoin";

export function BlockView(block: Block) {
	const { a, article, header, h1, dl, dt, dd, div, section, h2, code, time } = tags;

	const hash = block.header.hash().toReversed();
	const hashHex = encodeHex(hash);
	const prevHashHex = formatHash(block.header.prevHash);
	const timestamp = new Date(block.header.timestamp * SECOND);

	// Summary (tx count, reward, coinbase scriptSig) now lives in block.info.
	const info = block.info;

	const row = (term: string, ...value: Parameters<ReturnType<typeof dd>["append$"]>) =>
		div().append$(dt().textContent(term), dd().append$(...value));

	const self = article().$bind(BlockViewStyle.useScope());

	self.append$(
		header().append$(
			div({ class: "eyebrow" }).textContent("Block"),
			h1().textContent(formatBlockHeight(block.height)),
			code({ class: "hash" }).textContent(hashHex),
		),
		section().append$(
			h2().textContent("Header"),
			dl().append$(
				row("Version", formatBlockVersion(block.header.version)),
				row("Timestamp", time().dateTime(timestamp.toISOString()).textContent(formatDateTime(timestamp))),
				row("Difficulty", formatDifficulty(difficultyFromHeader(block.header))),
				row("Bits", `0x${block.header.bits.toString(16)}`),
				row("Nonce", `${block.header.nonce}`),
				row("Size", info ? formatBytesDecimal(info.wireSize) : "unknown"),
				row("Merkle root", code({ class: "hash" }).textContent(formatHash(block.header.merkleRoot))),
				row(
					"Previous block",
					a({ class: "hash" }).href(`#/block/${prevHashHex}`).textContent(prevHashHex),
				),
			),
		),
		section().append$(
			h2().textContent("Summary"),
			dl().append$(
				info
					? div({ class: "summary-rows" }).append$(
						row("Transactions", formatBlockHeight(info.txCount)),
						row("Reward", formatBitcoin(BigInt(info.reward))),
						row(
							"Coinbase",
							code({ class: "hash" }).textContent(
								info.coinbaseScriptSig.length ? new TextDecoder().decode(info.coinbaseScriptSig) : "(empty)",
							),
						),
					)
					: div({ class: "empty" }).textContent("No summary available"),
			),
		),
	);

	return self;
}

const BlockViewStyle = css`
	:scope {
		display: block grid;
		gap: 1.5em;
		align-content: start;
		padding-block: 1.5em;
		padding-inline: 1.25em;
		inline-size: 100%;
		max-inline-size: 60em;
	}

	header {
		display: block grid;
		gap: 0.35em;
		padding-block: 1.35em;
		padding-inline: 1.25em;
		border-radius: var(--panel-radius);
		background-image: var(--panel-surface);
		box-shadow: var(--panel-shadow);
	}

	.eyebrow {
		font-size: 0.7em;
		letter-spacing: 0.16em;
		text-transform: uppercase;
		color: color-mix(in srgb, currentcolor 50%, transparent);
	}

	h1 {
		font-size: 2.4em;
		line-height: 1;
		font-variant-numeric: tabular-nums;
		background-image: linear-gradient(180deg, var(--pop), color-mix(in srgb, var(--pop), var(--base) 45%));
		background-clip: text;
		color: transparent;
	}

	h2 {
		display: block grid;
		grid-template-columns: auto minmax(0, 1fr);
		align-items: center;
		gap: 0.75em;
		font-size: 0.8em;
		letter-spacing: 0.14em;
		text-transform: uppercase;
		color: color-mix(in srgb, currentcolor 60%, transparent);
	}

	h2::after {
		content: "";
		block-size: 1px;
		background-image: linear-gradient(to right, color-mix(in srgb, currentcolor 20%, transparent), transparent);
	}

	.hash {
		font-size: 0.85em;
		word-break: break-all;
		color: color-mix(in srgb, currentcolor 82%, transparent);
	}

	a.hash:hover {
		color: var(--accent-base);
	}

	section {
		display: block grid;
		gap: 0.9em;
		padding-block: 1.1em;
		padding-inline: 1.15em;
		border-radius: var(--panel-radius);
		background-image: var(--panel-surface);
		box-shadow: var(--panel-shadow);
	}

	dl,
	.summary-rows {
		display: block grid;
		gap: 0.65em 1.25em;
		grid-template-columns: repeat(auto-fit, minmax(min(100%, 16em), 1fr));
	}

	dl > div,
	.summary-rows > div {
		display: block grid;
		gap: 0.15em;
		overflow: hidden;
	}

	dt {
		font-size: 0.65em;
		letter-spacing: 0.14em;
		text-transform: uppercase;
		color: color-mix(in srgb, currentcolor 50%, transparent);
	}

	dd {
		font-variant-numeric: tabular-nums;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.empty {
		font-size: 0.85em;
		color: color-mix(in srgb, currentcolor 55%, transparent);
	}
`;
