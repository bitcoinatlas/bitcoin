import { sync, tags, toChild } from "@purifyjs/core";
import { encodeHex } from "@std/encoding";
import { api } from "~/ui/api.ts";
import { awaited } from "~/ui/utils/awaited.ts";
import { useReplaceChildren } from "~/ui/utils/bind.ts";
import { css } from "~/ui/utils/css.ts";
import { formatBtc, formatHash, formatLocktime, formatSequence } from "~/ui/utils/format.ts";
import type { WireTx } from "~/lib/codec/wire/WireTx.ts";
import type { StoredTx } from "~/lib/codec/stored/StoredTx.ts";
import appCss from "./app.css" with { type: "text" };

const appSheet = new CSSStyleSheet();
appSheet.replaceSync(appCss);
document.adoptedStyleSheets.push(appSheet);

function BlockDetailsContent(height: number | null) {
	const { p, div, dl, dt, dd, ul, li } = tags;

	if (height === null) {
		return p().textContent("Select a block");
	}

	const title = p().textContent(`Block ${height}`);

	const promise = Promise.all([
		api.fetch("GET /v1/block/:hashOrHeight", { pathname: { hashOrHeight: String(height) } }),
		api.fetch("GET /v1/block/:hashOrHeight/txs", { pathname: { hashOrHeight: String(height) } }),
	]);

	const content = div().$bind(useReplaceChildren(awaited(promise.then(([block, txs]) => {
		if (!block) return p().textContent("Block not found");

		const h = block.header;
		const timestamp = new Date(h.timestamp * 1000).toISOString();

		const headerSection = dl().append$(
			dt().textContent("Hash"),
			dd().textContent(formatHash(h.hash)),
			dt().textContent("Height"),
			dd().textContent(String(block.height)),
			dt().textContent("Time"),
			dd().textContent(timestamp),
			dt().textContent("Merkle Root"),
			dd().textContent(formatHash(h.merkleRoot)),
			dt().textContent("Prev Hash"),
			dd().textContent(formatHash(h.prevHash)),
			dt().textContent("Version"),
			dd().textContent("0x" + h.version.toString(16)),
			dt().textContent("Bits"),
			dd().textContent("0x" + h.bits.toString(16)),
			dt().textContent("Nonce"),
			dd().textContent(String(h.nonce)),
			dt().textContent("Tx count"),
			dd().textContent(String(txs.length)),
		);

		const txsSection = ul().append$(
			...txs.map((tx: { wire: WireTx; stored: StoredTx }, i: number) => li().append$(TxRow(tx, i))),
		);

		return div().append$(headerSection, txsSection);
	}))));

	return div().append$(title, content);
}

function TxRow(tx: { wire: WireTx; stored: StoredTx }, index: number) {
	const { details, summary, dl, dt, dd, ul, li, span } = tags;

	const totalOut = tx.wire.outputs.reduce((acc, o) => acc + o.value, 0n);
	const isCoinbase = tx.wire.inputs.length === 1 &&
		tx.wire.inputs[0]!.prevOut.txId.every((b) => b === 0) &&
		tx.wire.inputs[0]!.prevOut.vout === 0xffffffff;

	return details().append$(
		summary().append$(
			span().textContent(`#${index} `),
			span().textContent(formatHash(tx.wire.txId)),
			span().textContent(
				` | ${tx.wire.inputs.length} in, ${tx.wire.outputs.length} out | ${formatBtc(totalOut)}`,
			),
			isCoinbase ? span().textContent(" [coinbase]") : null,
		),
		dl().append$(
			dt().textContent("TxID"),
			dd().textContent(formatHash(tx.wire.txId)),
			dt().textContent("Version"),
			dd().textContent(String(tx.wire.version)),
			dt().textContent("Locktime"),
			dd().textContent(formatLocktime(tx.wire.locktime)),
			dt().textContent("Segwit"),
			dd().textContent(tx.wire.witness.length > 0 ? "yes" : "no"),
		),
		details().append$(
			summary().textContent(`Inputs (${tx.wire.inputs.length})`),
			ul().append$(
				...tx.wire.inputs.map((inp, i) => {
					const coinbaseInput = isCoinbase && i === 0;
					const storedIn = tx.stored.vin[i];
					return li().append$(
						dl().append$(
							dt().textContent("Index"),
							dd().textContent(String(i)),
							dt().textContent("Prev TxID"),
							dd().textContent(coinbaseInput ? "coinbase" : formatHash(inp.prevOut.txId)),
							dt().textContent("Vout"),
							dd().textContent(coinbaseInput ? "-" : String(inp.prevOut.vout)),
							dt().textContent("ScriptSig"),
							dd().textContent(encodeHex(inp.scriptSig)),
							dt().textContent("Sequence"),
							dd().textContent(formatSequence(inp.sequence)),
							...(tx.wire.witness[i] && tx.wire.witness[i]!.length > 0
								? [
									dt().textContent("Witness"),
									dd().textContent(tx.wire.witness[i]!.map((w) => encodeHex(w)).join(", ")),
								]
								: []),
							...(storedIn
								? [
									dt().textContent("Stored PrevOut"),
									dd().textContent(JSON.stringify(storedIn.prevOut.txId, (_k, v) =>
										v instanceof Uint8Array ? encodeHex(v) : v)),
								]
								: []),
						),
					);
				}),
			),
		),
		details().append$(
			summary().textContent(`Outputs (${tx.wire.outputs.length})`),
			ul().append$(
				...tx.wire.outputs.map((out, i) => {
					const storedOut = tx.stored.vout[i];
					return li().append$(
						dl().append$(
							dt().textContent("Index"),
							dd().textContent(String(i)),
							dt().textContent("Value"),
							dd().textContent(formatBtc(out.value)),
							dt().textContent("ScriptPubKey"),
							dd().textContent(encodeHex(out.scriptPubKey)),
							...(storedOut
								? [
									dt().textContent("Stored ScriptPubKey"),
									dd().textContent(JSON.stringify(storedOut.scriptPubKey, (_k, v) =>
										v instanceof Uint8Array ? encodeHex(v) : v)),
									dt().textContent("Spent"),
									dd().textContent(storedOut.spentBy ? "yes" : "no"),
								]
								: []),
						),
					);
				}),
			),
		),
	);
}

export function App() {
	const { body, ul, li, button, section } = tags;
	const self = body().$bind(appStyle.useScope());

	const tipPromise = api.fetch("GET /v1/block/tip", {});
	const blocksPromise = tipPromise.then((tip) => api.fetch("GET /v1/block", { search: { to: tip?.height ?? 0 } }));

	const selectedBlockHeight = sync<number | null>((set) => {
		set(null);
		const onHashChange = () => {
			const height = Number(location.hash.slice(1));
			set(isNaN(height) || !location.hash ? null : height);
		};
		onHashChange();
		globalThis.addEventListener("hashchange", onHashChange);
		return () => globalThis.removeEventListener("hashchange", onHashChange);
	});

	const blockList = section().id("blocklist").ariaLabel("Block List").append$(
		ul().$bind(useReplaceChildren(awaited(blocksPromise.then((blocks) =>
			blocks.map((block) => {
				return li().append$(
					button().type("button")
						.onclick(() => location.assign(new URL(`#${block.height}`, location.href)))
						.textContent(`${block.height}: ${formatHash(block.header.hash)}`),
				);
			})
		)))),
	);

	const blockDetails = section().id("blockdetails")
		.ariaLabel("Block Details")
		.$bind(useReplaceChildren(selectedBlockHeight.derive((height) => BlockDetailsContent(height))));

	self.append$(blockList, blockDetails);

	return self;
}

const appStyle = css`
	:scope {
		display: block grid;
		grid-template-columns: max-content 1fr;
		gap: 1em;
		align-items: start;
	}

	#blocklist {
		display: block grid;
		gap: 0.5em;
		overflow-y: auto;
		max-block-size: 100dvb;
	}

	#blockdetails {
		display: block grid;
		gap: 0.5em;
		overflow-x: auto;
	}

	dl {
		display: block grid;
		grid-template-columns: max-content 1fr;
		gap: 1em;
		font-size: 0.85em;
	}

	dt {
		font-weight: bold;
		opacity: 0.7;
	}

	dd {
		margin: 0;
		word-break: break-all;
	}

	ul {
		list-style: none;
		padding: 0;
		margin: 0;
		display: block grid;
		gap: 0.5em;
	}

	details {
		border: 1px solid #444;
	}

	summary {
		padding-inline: 0.25em;
		padding-block: 0.5em;
		cursor: pointer;
		word-break: break-all;
	}
`;

document.body.replaceWith(toChild(App()));
