import { Builder, sync, tags } from "@purifyjs/core";
import { encodeHex } from "@std/encoding";
import { api } from "~/app/frontend/api.ts";
import { BlockCard } from "~/app/frontend/components/BlockCard.ts";
import { useAttribute } from "~/app/frontend/utils/bind.ts";
import { css } from "~/app/frontend/utils/css.ts";
import { Block } from "~/app/routes.ts";

const OVERSCAN = 4; // rows rendered beyond the viewport on each side
const PAGE = 32; // window only moves in steps of this many rows
const DELAY = 500; // fetch fires this long after scrolling stops
const CACHE = 1024; // fetched blocks kept for instant refill on the way back

// Ruler gradations, in blocks. Minor lines come from a gradient, major ones
// carry a label.
const MINOR = 10_000;
const MAJOR = 100_000;

const THUMB = 1.5; // em — fixed; a proportional thumb would be sub-pixel

// Slack on each side of the viewport, in px of native scroll travel. The chain
// is taller than any browser's max scroll height, so the spacer only ever holds
// a window of travel and gets recentred once drift eats RECENTER_AT of it.
// Writing scrollTop is the one thing that interrupts an in-flight fling, so the
// window is big enough that it rarely happens.
const SLACK = 5_000_000;
const RECENTER_AT = 0.75;

const REDUCED = matchMedia("(prefers-reduced-motion: reduce)");

export async function ChainTimeline() {
	const tip = await api.fetch("GET /v1/block/tip", {});
	if (!tip) return;
	const total = tip.height + 1;

	const LIST_ID = `chain-timeline-blocks-${encodeHex(crypto.getRandomValues(new Uint8Array(4)))}`;

	const { section, div, span, ol, li } = tags;

	const self = section().$bind(ChainTimelineStyle.useScope());
	self.$node.tabIndex = 0; // the native scrollbar is hidden; keep it keyboard-scrollable
	self.$node.setAttribute("aria-label", "Block timeline");

	// In flow, owns the whole scroll range, and is the containing block the
	// sticky children are measured against. Out of flow it would leave them
	// bounded by a viewport-tall box and they would scroll away after a screen.
	const spacer = div();

	// Sticky and zero-height: rows flow out of it but it stays pinned to the
	// viewport block-start, so the transform only carries the row offset and
	// never has to cancel out scrollTop.
	const list = ol();
	list.$node.id = LIST_ID;
	list.$node.setAttribute("aria-label", "Blocks, newest first");

	// Scrollbar. Built rather than an <input type="range">: a vertical range
	// depends on engine-specific pseudo-elements for every part of its
	// appearance, and its writing mode inverts what the logical properties mean
	// on its own box. role="scrollbar" is the accurate role, the ARIA value
	// properties carry the position, and the keyboard handling is ten lines.
	// Selecting on [role] also keeps these rules at 0,1,0 rather than 0,0,1,
	// which a bare element selector loses to any global form or list reset.
	const bar = div();
	bar.$node.setAttribute("role", "scrollbar");
	bar.$node.setAttribute("aria-orientation", "vertical");
	bar.$node.setAttribute("aria-controls", LIST_ID);
	bar.$node.setAttribute("aria-valuemin", "0");
	bar.$node.setAttribute("aria-valuemax", String(tip.height));
	bar.$node.tabIndex = 0;
	bar.$node.style.setProperty("--minor", `${(MINOR / tip.height) * 100}%`);
	bar.$node.style.setProperty("--major", `${(MAJOR / tip.height) * 100}%`);

	const thumb = div();

	// Purely visual: the position is already on the scrollbar as aria-valuetext,
	// so this must not be announced a second time.
	const readout = span();
	readout.$node.setAttribute("aria-hidden", "true");
	thumb.append$(readout);

	const mark = (height: number) => {
		const el = span();
		el.$node.textContent = height === 0 ? "0" : `${Math.round(height / 1000)}k`;
		el.$node.style.setProperty("--at", String((tip.height - height) / tip.height));
		return el;
	};

	// Round gradations counted up from genesis, then the tip itself — which is
	// not a round number and gets its own mark at the very top.
	for (let height = 0; height < tip.height; height += MAJOR) bar.append$(mark(height));
	bar.append$(mark(tip.height));
	bar.append$(thumb);

	// --- state ------------------------------------------------------------
	let viewport = 0;
	let itemHeight = 0;
	let maxFirst = 0;
	let position = 0; // fractional row index at the viewport block-start

	let anchor = 0;
	let restTop = 0;
	let slackTop = 0;
	let slackBottom = 0;
	let syncing = false;

	type Row = { member: Builder<HTMLElement>; node: HTMLElement; height: number };
	const pool: Row[] = [];
	const cache = new Map<number, Block>();
	let first = -1;
	let count = 0;

	let busy: ((value: string) => void) | null = null;
	let timer: ReturnType<typeof setTimeout>;
	let idle: ReturnType<typeof setTimeout>;
	let token = 0;

	// Labels and readout surface while scrubbing, then fade back out.
	const activate = () => {
		bar.$node.setAttribute("data-active", "");
		clearTimeout(idle);
		idle = setTimeout(() => bar.$node.removeAttribute("data-active"), 700);
	};

	// --- rows -------------------------------------------------------------
	// A fixed pool, appended once and reused in place. Rows are never removed
	// and re-added: a node that leaves the DOM drops its running transition and
	// comes back at the end state, which is what kills the reveal. A waiting row
	// is simply empty and aria-busy — the placeholder is drawn by ::before.
	const rowAt = (height: number) => {
		const index = tip.height - height - first;
		const row = pool[index];
		return row && index < count && row.height === height ? row : null;
	};

	// The card is another component and brings its own [data-scope] — exactly
	// where this scope's lower boundary stops — so no rule in the stylesheet can
	// reach it and the reveal has to be set inline. `instant` is for rows
	// refilled from cache: they never left the screen, so they should not
	// re-reveal.
	const fill = (row: Row, block: Block, instant: boolean) => {
		row.node.replaceChildren();
		row.member.append$(BlockCard(tip.height, block));
		row.node.style.removeProperty("block-size");
		row.node.removeAttribute("aria-busy");
		if (instant || REDUCED.matches) return;

		for (const child of row.node.children) {
			const card = child as HTMLElement;
			card.style.setProperty("transition", "opacity 560ms ease-out");
			card.style.setProperty("opacity", "0");
			requestAnimationFrame(() => card.style.removeProperty("opacity"));
		}
	};

	const clear = (row: Row) => {
		row.node.replaceChildren();
		row.node.setAttribute("aria-busy", "true");
		row.node.style.setProperty("block-size", itemHeight > 0 ? `${itemHeight}px` : "var(--row-size)");
	};

	// Returns whether the window moved, i.e. whether a fetch is now owed.
	const paint = () => {
		const visible = itemHeight > 0 && viewport > 0 ? Math.ceil(viewport / itemHeight) : PAGE;
		const take = Math.min(total, visible + PAGE + OVERSCAN * 2);
		const start = Math.max(0, Math.min(total - take, Math.floor(position / PAGE) * PAGE - OVERSCAN));
		if (start === first && take === count) return false;

		while (pool.length < take) {
			const member = li();
			const node = member.$node as HTMLElement;
			node.setAttribute("aria-setsize", String(total));
			pool.push({ member, node, height: -1 });
			list.append$(member);
		}

		first = start;
		count = take;

		for (let i = 0; i < pool.length; i++) {
			const row = pool[i]!;
			if (i >= take) {
				row.node.hidden = true;
				continue;
			}
			row.node.hidden = false;

			const height = tip.height - (start + i);
			if (row.height === height) continue;
			row.height = height;
			row.node.setAttribute("aria-posinset", String(start + i + 1));

			const block = cache.get(height);
			if (block) fill(row, block, true);
			else clear(row);
		}

		return true;
	};

	const load = () => {
		let owed = false;
		for (let i = 0; i < count && !owed; i++) owed = pool[i]!.node.hasAttribute("aria-busy");
		if (!owed) return;

		const to = tip.height - first;
		const take = count;
		const current = ++token;
		busy?.("true");

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
				busy?.("false");
				// Measure off the cards that just landed instead of waiting for a
				// resize — until then every row still sits at the --row-size guess.
				requestAnimationFrame(measure);
			})
			.catch((error) => console.error(error));
	};

	const schedule = (delay = DELAY) => {
		clearTimeout(timer);
		timer = setTimeout(load, delay);
	};

	// --- scrolling --------------------------------------------------------
	const layout = (target: number) => {
		position = Math.min(maxFirst, Math.max(0, target));
		slackTop = Math.min(SLACK, position * itemHeight);
		slackBottom = Math.min(SLACK, (maxFirst - position) * itemHeight);
		anchor = position;
		restTop = slackTop;

		spacer.$node.style.setProperty("block-size", `${slackTop + viewport + slackBottom}px`);
		syncing = true;
		self.$node.scrollTop = restTop;
		syncing = false;
	};

	const update = () => {
		if (paint()) schedule();
		list.$node.style.setProperty("transform", `translateY(${(first - position) * itemHeight}px)`);

		// Same denominator the ruler uses, so the thumb centre lands on the line
		// it points at. maxFirst would shift it by about a viewport.
		const at = Math.min(1, Math.max(0, position / Math.max(1, tip.height)));
		const top = tip.height - Math.round(position);
		// Past halfway the bottom edge is the one you are aiming at, so read that
		// out instead — otherwise the end of the track never says 0.
		const visible = itemHeight > 0 ? Math.max(1, Math.ceil(viewport / itemHeight)) : 1;
		const height = maxFirst > 0 && position / maxFirst > 0.5 ? Math.max(0, top - (visible - 1)) : top;
		thumb.$node.style.setProperty("--at", String(at));
		readout.$node.textContent = height.toLocaleString();
		bar.$node.setAttribute("aria-valuenow", String(Math.round(position)));
		bar.$node.setAttribute("aria-valuetext", `Block ${height.toLocaleString()}`);
	};

	const measure = () => {
		const nextViewport = self.$node.clientHeight;

		// Only a settled row is authoritative: a waiting row is sized *from*
		// itemHeight, so measuring one is a fixed point at whatever we started
		// with, including 0. The box includes its padding-block, so the gap is
		// part of the pitch and the math stays one number.
		let next = itemHeight;
		const settled = list.$node.querySelector("li:not([aria-busy]):not([hidden])");
		if (settled) next = Math.round(settled.getBoundingClientRect().height) || itemHeight;

		if (next <= 0 || nextViewport <= 0) return;
		if (next === itemHeight && nextViewport === viewport) return;

		itemHeight = next;
		viewport = nextViewport;
		// Fractional on purpose: rounding up leaves the last row hanging below
		// the fold by whatever the viewport is not an exact multiple of.
		maxFirst = Math.max(0, total - viewport / itemHeight);
		for (let i = 0; i < count; i++) {
			const row = pool[i]!;
			if (row.node.hasAttribute("aria-busy")) row.node.style.setProperty("block-size", `${itemHeight}px`);
		}
		layout(position);
		update();
	};

	const onScroll = () => {
		if (syncing || itemHeight <= 0) return;
		const drift = self.$node.scrollTop - restTop;
		const next = Math.min(maxFirst, Math.max(0, anchor + drift / itemHeight));
		if (drift < -slackTop * RECENTER_AT || drift > slackBottom * RECENTER_AT) layout(next);
		else position = next;
		activate();
		update();
	};

	// Absolute positioning for both the click and the drag: wherever the pointer
	// is, that is where the thumb goes. No grab offset to track.
	const rowsAt = (clientY: number) => {
		const rail = bar.$node.getBoundingClientRect();
		const size = thumb.$node.getBoundingClientRect().height;
		const travel = rail.height - size;
		if (travel <= 0) return 0;
		return ((clientY - rail.top - size / 2) / travel) * maxFirst;
	};

	// One signal, owning setup and teardown, publishing the flag it names.
	list.$bind(useAttribute(
		"aria-busy",
		sync<string>((set) => {
			busy = set;
			set("false");

			const aborter = new AbortController();
			const { signal } = aborter;

			self.$node.addEventListener("scroll", onScroll, { passive: true, signal });

			bar.$node.addEventListener("pointerdown", (event: PointerEvent) => {
				bar.$node.setPointerCapture(event.pointerId);
				layout(rowsAt(event.clientY));
				activate();
				update();
				event.preventDefault();
			}, { signal });

			bar.$node.addEventListener("pointermove", (event: PointerEvent) => {
				if (!bar.$node.hasPointerCapture(event.pointerId)) return;
				layout(rowsAt(event.clientY));
				activate();
				update();
			}, { signal });

			bar.$node.addEventListener("keydown", (event: KeyboardEvent) => {
				const page = Math.max(1, Math.ceil(viewport / Math.max(1, itemHeight)));
				let next = position;
				if (event.key === "ArrowDown") next += 1;
				else if (event.key === "ArrowUp") next -= 1;
				else if (event.key === "PageDown") next += page;
				else if (event.key === "PageUp") next -= page;
				else if (event.key === "Home") next = 0;
				else if (event.key === "End") next = maxFirst;
				else return;
				event.preventDefault();
				layout(next);
				activate();
				update();
			}, { signal });

			const observer = new ResizeObserver(measure);
			observer.observe(self.$node);

			update();
			schedule(0);

			return () => {
				aborter.abort();
				observer.disconnect();
				clearTimeout(timer);
				clearTimeout(idle);
				busy = null;
			};
		}),
	));

	spacer.append$(list, bar);
	self.append$(spacer);
	return self;
}

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

	> div {
		position: relative;
	}

	ol {
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
		display: block;
		box-sizing: border-box;
		padding-block: var(--row-gap);
	}

	li[hidden] {
		display: none;
	}

	li::before {
		content: "";
		position: absolute;
		inset-block: var(--row-gap);
		inset-inline: 0;
		border-radius: var(--row-radius);
		background-color: var(--row-placeholder);
		opacity: 0;
		transition: opacity 300ms ease-out 60ms;
	}

	li[aria-busy]::before {
		opacity: 1;
	}

	[role="scrollbar"] {
		position: sticky;
		inset-block-start: 0;
		z-index: 2;
		margin-inline-start: auto;
		inline-size: 1.25em;
		block-size: 100cqb;
		touch-action: none;
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
		background-size:
			1.15em calc(100% - ${THUMB}em),
			0.9em calc(100% - ${THUMB}em),
			0.4em calc(100% - ${THUMB}em);
		background-position: 100% ${THUMB / 2}em;
	}

	[role="scrollbar"] > div {
		position: absolute;
		inset-inline: 0.15em;
		inset-block-start: calc((100% - ${THUMB}em) * var(--at, 0));
		block-size: ${THUMB}em;
		border-radius: 0.75em;
		background-color: currentColor;
		opacity: 0.55;
		box-shadow: 0 0.0625em 4px color-mix(in srgb, currentColor 35%, transparent);
		transition: opacity 150ms ease;
	}

	[role="scrollbar"]:hover > div,
	[role="scrollbar"][data-active] > div,
	[role="scrollbar"]:focus-visible > div {
		opacity: 0.9;
	}

	[role="scrollbar"] > span {
		position: absolute;
		inset-inline-end: 1.6em;
		inset-block-start: calc((100% - ${THUMB}em) * var(--at, 0) + ${THUMB / 2}em);
		translate: 0 -50%;
		font-size: 0.65em;
		line-height: 1;
		white-space: nowrap;
		font-variant-numeric: tabular-nums;
		opacity: 0.3;
		transition: opacity 150ms ease;
	}

	[role="scrollbar"]:hover > span,
	[role="scrollbar"][data-active] > span {
		opacity: 0.65;
	}

	[role="scrollbar"] > div > span {
		position: absolute;
		inset-inline-end: 1.6em;
		inset-block-start: 50%;
		translate: 0 -50%;
		padding-inline: 0.5em;
		padding-block: 0.25em;
		border-radius: 0.375em;
		font-size: 0.75em;
		line-height: 1;
		white-space: nowrap;
		font-variant-numeric: tabular-nums;
		background-color: canvas;
		color: canvastext;
		box-shadow: 0 0.125em 8px rgb(0 0 0 / 0.25);
		opacity: 0;
		transition: opacity 150ms ease;
	}

	[role="scrollbar"]:hover > div > span,
	[role="scrollbar"][data-active] > div > span,
	[role="scrollbar"]:focus-visible > div > span {
		opacity: 1;
	}

	@media (prefers-reduced-motion: reduce) {
		li::before {
			transition: none;
		}
	}
`;
