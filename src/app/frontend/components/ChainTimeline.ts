import { Builder, combine, ref, Sync, tags } from "@purifyjs/core";
import { api } from "~/app/frontend/api.ts";
import { BlockCard } from "~/app/frontend/components/BlockCard.ts";
import { useClassToggle, useStyleProperty } from "~/app/frontend/utils/bind.ts";
import { css } from "~/app/frontend/utils/css.ts";
import { Block } from "~/app/routes.ts";
import { HALVING_BLOCKS } from "~/constants.ts";

const OVERSCAN = 4; // rows rendered beyond the viewport on each side
const PAGE = 32; // window only moves in steps of this many rows
const DELAY = 500; // fetch fires this long after scrolling stops
const CACHE = 1024; // fetched blocks kept for instant refill on the way back

// Ruler gradations, in blocks: minor lines come from a gradient, major ones carry a label.
const MINOR = HALVING_BLOCKS * 0.05;
const MAJOR = HALVING_BLOCKS;

// Slack on each side of the viewport, in px of native scroll travel. The chain is taller
// than any browser's max scroll height, so the spacer only ever holds a window of travel and
// gets recentred once drift eats RECENTER_AT of it. Writing scrollTop interrupts an in-flight
// fling, so the window is big enough that it rarely happens.
const SLACK = 5_000_000;
const RECENTER_AT = 0.75;

const REDUCED = matchMedia("(prefers-reduced-motion: reduce)");

const { section, div, span, ol, li, input } = tags;

export async function ChainTimeline() {
	const tip = await api.fetch("GET /v1/block/tip", {});
	if (!tip) return;
	const total = tip.height + 1;

	// The value surface: position is the single source of truth, maxFirst bounds it, busy is
	// the fetch flag. The scrollbar and every derived readout follow these.
	const position = ref(0); // fractional row index at the viewport block-start
	const maxFirst = ref(0);
	const busy = ref<"true" | "false">("false");

	// Sticky, zero-height, grid: rows flow out of it but it stays pinned to the viewport top,
	// so the transform only carries the row offset. Fixed grid tracks give a constant pitch,
	// so nothing waits on a card to know how tall a row is.
	const list = ol().ariaLabel("Blocks, newest first").ariaBusy(busy);

	// In flow, owns the whole scroll range, and is the containing block the sticky children
	// are measured against.
	const spacer = div();

	const self = section().$bind(ChainTimelineStyle.useScope())
		.tabIndex(0) // the native scrollbar is hidden; keep it keyboard-scrollable
		.ariaLabel("Block timeline");

	// --- mechanics: plain numbers, read every scroll frame ----------------
	let viewport = 0;
	let itemHeight = 0;
	let anchor = 0;
	let restTop = 0;
	let slackTop = 0;
	let slackBottom = 0;
	let syncing = false;

	type Row = { item: Builder<HTMLElement>; height: number };
	const pool: Row[] = [];
	const cache = new Map<number, Block>();
	let first = -1;
	let count = 0;

	let timer: ReturnType<typeof setTimeout>;
	let token = 0;

	// --- rows -------------------------------------------------------------
	// A fixed pool, appended once and reused in place. Rows are never removed and re-added: a
	// node that leaves the DOM drops its running animation and comes back at the end state. A
	// waiting row is simply empty and aria-busy — the placeholder is drawn by ::before.
	const rowAt = (height: number) => {
		const index = tip.height - height - first;
		const row = pool[index];
		return row && index < count && row.height === height ? row : null;
	};

	// `instant` is for rows refilled from cache: they never left the screen, so they should not
	// re-reveal. The card owns its own [data-scope], so the reveal is set per node via WAAPI.
	const fill = (row: Row, block: Block, instant: boolean) => {
		row.item.replaceChildren$(BlockCard(tip.height, block)).ariaBusy(null);
		if (instant || REDUCED.matches) return;
		for (const card of row.item.$node.children) {
			card.animate({ opacity: [0, 1] }, { duration: 560, easing: "ease-out" });
		}
	};

	const clear = (row: Row) => {
		row.item.replaceChildren().ariaBusy("true");
	};

	// Returns whether the window moved, i.e. whether a fetch is now owed.
	const paint = () => {
		const visible = itemHeight > 0 && viewport > 0 ? Math.ceil(viewport / itemHeight) : PAGE;
		const take = Math.min(total, visible + PAGE + OVERSCAN * 2);
		const start = Math.max(0, Math.min(total - take, Math.floor(position.val / PAGE) * PAGE - OVERSCAN));
		if (start === first && take === count) return false;

		while (pool.length < take) {
			const item = li().ariaSetSize(`${total}`);
			pool.push({ item, height: -1 });
			list.append$(item);
		}

		first = start;
		count = take;

		for (let i = 0; i < pool.length; i++) {
			const row = pool[i]!;
			if (i >= take) {
				row.item.hidden(true);
				continue;
			}
			row.item.hidden(false);

			const height = tip.height - (start + i);
			if (row.height === height) continue;
			row.height = height;
			row.item.ariaPosInSet(`${start + i + 1}`);

			const block = cache.get(height);
			if (block) fill(row, block, true);
			else clear(row);
		}

		return true;
	};

	const load = () => {
		let owed = false;
		for (let i = 0; i < count && !owed; i++) owed = pool[i]!.item.$node.ariaBusy !== null;
		if (!owed) return;

		const to = tip.height - first;
		const take = count;
		const current = ++token;
		busy.set("true");

		api.fetch("GET /v1/block?to=:to&take=:take", { params: { search: { to, take } } })
			.then((blocks) => {
				if (current !== token) return;
				blocks.slice().reverse().forEach((block, i) => {
					const height = to - i;
					cache.delete(height);
					cache.set(height, block);
					const row = rowAt(height);
					if (row) fill(row, block, false);
				});
				while (cache.size > CACHE) cache.delete(cache.keys().next().value!);
				busy.set("false");
			})
			.catch((error) => console.error(error));
	};

	const schedule = (delay = DELAY) => {
		clearTimeout(timer);
		timer = setTimeout(load, delay);
	};

	// --- scrolling --------------------------------------------------------
	// Re-anchors the native scroll window around a target position (used by the scrollbar and
	// on resize). Native scroll itself doesn't come through here unless it drifts far enough to
	// need recentring.
	const layout = (target: number) => {
		const clamped = Math.min(maxFirst.val, Math.max(0, target));
		position.set(clamped);
		slackTop = Math.min(SLACK, clamped * itemHeight);
		slackBottom = Math.min(SLACK, (maxFirst.val - clamped) * itemHeight);
		anchor = clamped;
		restTop = slackTop;

		spacer.$node.style.setProperty("block-size", `${slackTop + viewport + slackBottom}px`);
		syncing = true;
		self.$node.scrollTop = restTop;
		syncing = false;
	};

	// Windows the row strip and lays it under the fold. The scrollbar follows `position` on its
	// own — the only thing left to write imperatively is the transform.
	const render = () => {
		if (paint()) schedule();
		list.$node.style.setProperty("transform", `translateY(${(first - position.val) * itemHeight}px)`);
	};

	const measure = () => {
		const nextViewport = self.$node.clientHeight;

		// Pitch = the top-delta of two adjacent rows, so the gap is included without reading any
		// computed style. Grid sizes every row the same whether it's filled or waiting.
		const a = pool[0]?.item.$node;
		const b = pool[1]?.item.$node;
		let next = itemHeight;
		if (a && b && !a.hidden && !b.hidden) {
			next = Math.round(b.getBoundingClientRect().top - a.getBoundingClientRect().top) || itemHeight;
		}

		if (next <= 0 || nextViewport <= 0) return;
		if (next === itemHeight && nextViewport === viewport) return;

		itemHeight = next;
		viewport = nextViewport;
		// Fractional on purpose: rounding up leaves the last row hanging below the fold.
		maxFirst.set(Math.max(0, total - viewport / itemHeight));
		layout(position.val);
		render();
	};

	const onScroll = () => {
		if (syncing || itemHeight <= 0) return;
		const drift = self.$node.scrollTop - restTop;
		const next = Math.min(maxFirst.val, Math.max(0, anchor + drift / itemHeight));
		if (drift < -slackTop * RECENTER_AT || drift > slackBottom * RECENTER_AT) layout(next);
		else position.set(next);
		render();
	};

	self.$bind((host) => {
		const aborter = new AbortController();
		host.addEventListener("scroll", onScroll, { passive: true, signal: aborter.signal });
		const observer = new ResizeObserver(measure);
		observer.observe(host);
		render();
		schedule(0);
		return () => {
			aborter.abort();
			observer.disconnect();
			clearTimeout(timer);
		};
	});

	return self.append$(
		spacer.append$(
			list,
			ChainScrollbar({
				value: position,
				max: maxFirst,
				tip: tip.height,
				controls: list.$node,
				onScrub: (value) => {
					layout(value);
					render();
				},
			}),
		),
	);
}

// A single ruler label. --mark-at is fixed at build, so it goes straight into the style
// attribute: 0 sits at the axis top (the tip), 1 at the bottom (genesis).
function mark(height: number, tip: number) {
	return div({ class: "mark", style: `--mark-at:${(tip - height) / tip}` })
		.append$(span().textContent(height === 0 ? "0" : `${Math.round(height / 1000)}k`));
}

// The scrollbar is a native <input type=range> relabelled as role=scrollbar. It owns the
// thumb, drag, click-to-scrub, keyboard, and value semantics; we only bridge its value to the
// position signal both ways and lay a decorative ruler behind it. Its own scope; the timeline
// can't reach past its [data-scope] boundary, and it positions itself against the container.
function ChainScrollbar(props: {
	value: Sync.Ref<number>; // position, in fractional rows; two-way with the slider
	max: Sync<number>; // maxFirst — the deepest the viewport can scroll
	tip: number; // tip height, for the ruler fractions and labels
	controls: Element; // what the scrollbar scrolls, for aria-controls
	onScrub: (value: number) => void; // user moved the thumb (drag/keys)
}) {
	// Fraction of the axis the thumb sits at, for the readout that rides it.
	const at = combine({ value: props.value, max: props.max })
		.derive(({ value, max }) => (max > 0 ? Math.min(1, Math.max(0, value / max)) : 0));
	// Block height under the thumb, for the readout and the announced value.
	const height = props.value.derive((v) => props.tip - Math.round(v));

	// Surface the ruler + readout while the value is moving (scroll or scrub), then fade — so
	// scrolling gets the same affordance as hovering, without a hover.
	const active = ref(false);
	let idle: ReturnType<typeof setTimeout>;
	const activate = () => {
		active.set(true);
		clearTimeout(idle);
		idle = setTimeout(() => active.set(false), 700);
	};

	const marks: Builder<HTMLElement>[] = [];
	for (let h = 0; h < props.tip; h += MAJOR) marks.push(mark(h, props.tip));
	marks.push(mark(props.tip, props.tip)); // the tip itself isn't a round number

	const slider = input({ type: "range" })
		.min("0")
		.max(props.max.derive((v) => String(Math.max(0, Math.ceil(v)))))
		.step("any")
		.role("scrollbar")
		.ariaOrientation("vertical")
		.ariaControlsElements([props.controls])
		.ariaValueText(height.derive((h) => `Block ${h.toLocaleString()}`))
		.$bind((el) => {
			const aborter = new AbortController();
			el.addEventListener("input", () => props.onScrub(el.valueAsNumber), { signal: aborter.signal });
			// Reflect scroll-driven changes back onto the thumb, but never fight a value the
			// slider already holds (mid-drag it's the source, not the sink). Any real change —
			// scroll, scrub, or keys — surfaces the ruler; the immediate bind call doesn't.
			let first = true;
			const unfollow = props.value.follow((v) => {
				if (el.valueAsNumber !== v) el.valueAsNumber = v;
				if (first) first = false;
				else activate();
			}, true);
			return () => {
				aborter.abort();
				unfollow();
				clearTimeout(idle);
			};
		});

	return div({
		class: "scrollbar",
		// Static gradations, as a fraction of the axis — they never change, so no binding.
		style: `--minor:${(MINOR / props.tip) * 100}%; --major:${(MAJOR / props.tip) * 100}%`,
	})
		.$bind(ChainScrollbarStyle.useScope())
		.$bind(useClassToggle({ active }))
		.append$(
			slider,
			div({ class: "ruler" }).ariaHidden("true")
				.$bind(useStyleProperty("at", at.derive(String)))
				.append$(marks, div({ class: "readout" }).append$(span().textContent(height.derive((h) => h.toLocaleString())))),
		);
}

const ChainScrollbarStyle = css`
	:scope {
		--thumb-inline-size: 1.5em; /* thumb size */
		--thumb-block-size: .65em; /* thumb size */
		--thumb-wall-gap: .25em;
		--track-pad: calc(var(--thumb-block-size) * .5); /* spacing at top and bottom */

		position: sticky;
		inset-block-start: 0;
		z-index: 2;
		margin-inline-start: auto;
		inline-size: 1.25em;
		block-size: 100cqb;
	}

	/* The native range: fills the box, runs vertically, its own track invisible so the ruler
	   shows through. It keeps the thumb, keyboard, and pointer behaviour. */
	[type="range"] {
		position: absolute;
		writing-mode: vertical-lr;
		inset-inline: calc(var(--track-pad) - calc(var(--thumb-block-size) * .5));
		inset-block-end: var(--thumb-wall-gap);
		block-size: var(--thumb-inline-size);
		appearance: none;
		background: transparent;
		touch-action: none;
		cursor: pointer;
		color: inherit;
	}

	[type="range"]::-webkit-slider-runnable-track,
	[type="range"]::-moz-range-track {
		background: transparent;
	}

	[type="range"]::-webkit-slider-thumb {
		appearance: none;
		block-size: var(--thumb-inline-size);
		inline-size: var(--thumb-block-size);
		border-radius: 0.75em;
		background-color: currentColor;
		opacity: 0.55;
		box-shadow: 0 0.0625em 4px color-mix(in srgb, currentColor 35%, transparent);
		transition: opacity 150ms ease;
	}

	[type="range"]::-moz-range-thumb {
		block-size: var(--thumb-inline-size);
		inline-size: var(--thumb-block-size);
		border: none;
		border-radius: 0.75em;
		background-color: currentColor;
		opacity: 0.55;
		box-shadow: 0 0.0625em 4px color-mix(in srgb, currentColor 35%, transparent);
		transition: opacity 150ms ease;
	}

	[type="range"]:hover::-webkit-slider-thumb,
	[type="range"]:focus-visible::-webkit-slider-thumb,
	:scope.active [type="range"]::-webkit-slider-thumb {
		opacity: 0.9;
	}

	[type="range"]:hover::-moz-range-thumb,
	[type="range"]:focus-visible::-moz-range-thumb,
	:scope.active [type="range"]::-moz-range-thumb {
		opacity: 0.9;
	}

	/* The value axis, as a box inset by the end gutter, so 100% here *is* the travel and the
	   gradation percentages resolve against it directly. */
	.ruler {
		position: absolute;
		inset-block: var(--track-pad);
		inset-inline: 0;
		z-index: 1;
		pointer-events: none;
		background-image:
			linear-gradient(
			to bottom,
			color-mix(in srgb, currentColor 60%, transparent) 0 0.0625em,
			transparent 0.0625em
		),
			repeating-linear-gradient(
			to top,
			color-mix(in srgb, currentColor 45%, transparent) 0 0.0625em,
			transparent 0.0625em var(--major)
		),
			repeating-linear-gradient(
			to top,
			color-mix(in srgb, currentColor 20%, transparent) 0 0.0625em,
			transparent 0.0625em var(--minor)
		);
		background-repeat: no-repeat;
		background-size: 0.9em 100%, 0.9em 100%, 0.4em 100%;
		background-position-x: 100%;
	}

	.ruler .mark {
		display: block flow;
		position: absolute;
		inset-inline-end: calc(var(--thumb-inline-size) + .15em);
		inset-block-start: calc(100% * var(--mark-at, 0));
		translate: 0 -50%;
		line-height: 1;
		white-space: nowrap;
		font-variant-numeric: tabular-nums;
		opacity: 0.3;
		transition: opacity 150ms ease;

		span {
			font-size: .65em;
		}
	}

	:scope:is(:hover, :focus-within, .active) .ruler .mark {
		opacity: 0.65;
	}

	.readout {
		display: block flow;
		position: absolute;
		inset-block-start: calc(100% * var(--at, 0));
		inset-inline-end: calc(var(--thumb-inline-size) + .15em);
		padding-inline: 0.5em;
		padding-block: 0.25em;
		border-radius: 0.375em;
		background-color: canvas;
		color: canvastext;
		box-shadow: 0 0.125em 8px rgb(0 0 0 / 0.25);
		opacity: 0;
		translate: 0 -50%;
		transition: opacity 150ms ease;

		span {
			font-size: .85em;
		}
	}

	:scope:is(:hover, :focus-within, .active) .readout {
		opacity: 1;
	}
`;

const ChainTimelineStyle = css`
	:scope {
		--row-size: 8.375em;
		--row-gap: 0.375em;
		--row-radius: 0.5em;
		--row-placeholder: color-mix(in srgb, currentColor 4%, transparent);

		display: block;
		container-type: size;
		overflow-block: auto;
		overflow-anchor: none;
		overscroll-behavior: contain;
		scrollbar-width: none;
	}

	:scope::-webkit-scrollbar {
		display: none;
	}

	:scope > div {
		position: relative;
	}

	ol {
		display: grid;
		grid-auto-rows: var(--row-size);
		row-gap: var(--row-gap);
		position: sticky;
		inset-block-start: 0;
		block-size: 0;
		z-index: 1;
		margin-block: 0;
		padding-inline: 0;
		list-style: none;
	}

	li {
		position: relative;
		min-block-size: 0;
		overflow: clip; /* a card outgrowing its track is a bug, not a reflow */
	}

	li[hidden] {
		display: none;
	}

	li::before {
		content: "";
		position: absolute;
		inset-block: 0;
		inset-inline: 0;
		border-radius: var(--row-radius);
		background-color: var(--row-placeholder);
		opacity: 0;
		transition: opacity 300ms ease-out 60ms;

		@media (prefers-reduced-motion: reduce) {
			transition: none;
		}
	}

	li[aria-busy]::before {
		opacity: 1;
	}
`;
