import { Builder, combine, type Lifecycle, ref, Sync, tags } from "@purifyjs/core";
import { api } from "~/app/frontend/api.ts";
import { BlockWidget } from "~/app/frontend/components/BlockWidget.ts";
import { ChainScrollbar } from "~/app/frontend/components/ChainScrollbar.ts";
import { awaited } from "~/app/frontend/utils/awaited.ts";
import { useReplaceChildren, useStyleProperty } from "~/app/frontend/utils/bind.ts";
import { css } from "~/app/frontend/utils/css.ts";
import { defer } from "~/app/frontend/utils/defer.ts";
import { unroll } from "~/app/frontend/utils/unroll.ts";
import { Block } from "~/app/routes.ts";

// The section scrolls NATIVELY — wheel, touch, momentum, keyboard and the accessible scroll
// semantics all belong to the browser. But the chain is taller than any browser's max scroll
// height, so the spacer can't be sized to the whole thing. Instead it holds only a WINDOW of
// travel (SLACK px either side of the viewport); when the native scrollTop drifts far enough
// into the slack we recentre it — reset scrollTop and re-anchor — so scrolling is effectively
// infinite. `position` (fractional row index at the viewport top, 0 = tip) is the logical
// position we read off the native scroll each frame, and every derived signal follows it.

const OVERSCAN = 4; // rows kept live beyond the viewport on each side
const PAGE = 32; // the live window only shifts in steps of this many rows
const DELAY = 300; // fetch fires this long after the window last moved (ms)
const CACHE = 1024; // blocks kept by height for instant refill on the way back

// Native scroll travel kept on each side of the viewport. Since the real height overflows the
// browser's limit, the spacer only ever holds this window; once drift eats RECENTER_AT of it we
// recentre. Big enough that a fling rarely reaches the edge mid-gesture.
const SLACK = 5_000_000;
const RECENTER_AT = 0.75;

const { section, div, ol, li } = tags;

// Mirror an element's block-size into a signal. Empty rows still report --row-size, so this is
// all we need for the row pitch (border-box) and the viewport height (content-box).
function useMeasureBlockSize(
	target: Sync.Ref<number>,
	box: "border-box" | "content-box" = "border-box",
): Lifecycle.OnConnected {
	return (element) => {
		const observer = new ResizeObserver(([entry]) => {
			const size = (box === "content-box" ? entry?.contentBoxSize : entry?.borderBoxSize)?.[0];
			target.set(size ? size.blockSize : 0);
		});
		observer.observe(element, { box });
		return () => observer.disconnect();
	};
}

export async function ChainTimeline() {
	const tip = await api.fetch("GET /v1/block/tip", {});
	if (!tip) return;
	const total = tip.height + 1; // heights 0..tip.height, newest first

	// --- measured geometry, in px ----------------------------------------
	const rowSize = ref(0); // row pitch (the whole li, gap baked in); 0 until measured
	const viewport = ref(0); // visible height of the scroll surface

	// --- scroll model, in fractional rows --------------------------------
	const position = ref(0);
	const maxFirst = combine({ rowSize, viewport })
		.derive(({ rowSize, viewport }) => (rowSize > 0 ? Math.max(0, total - viewport / rowSize) : 0));

	// --- the live window (quantised to PAGE with overscan) ---------------
	// take/start only change on a page crossing, so the row set rebuilds rarely; `offset` slides
	// every frame instead.
	const geometry = combine({ position, rowSize, viewport });
	const take = geometry.derive(({ rowSize, viewport }) => {
		if (rowSize <= 0) return 0;
		return Math.min(total, Math.ceil(viewport / rowSize) + PAGE + OVERSCAN * 2);
	});
	const start = geometry.derive(({ position, rowSize, viewport }) => {
		if (rowSize <= 0) return 0;
		const count = Math.min(total, Math.ceil(viewport / rowSize) + PAGE + OVERSCAN * 2);
		return Math.max(0, Math.min(total - count, Math.floor(position / PAGE) * PAGE - OVERSCAN));
	});
	const offset = combine({ position, start, rowSize })
		.derive(({ position, start, rowSize }) => (start - position) * rowSize);

	// --- fetch -----------------------------------------------------------
	// Filled blocks live keyed by height; `revision` ticks whenever the store changes so each
	// row's derive re-reads it. `defer` debounces the window so flinging doesn't hit the API;
	// awaited + unroll keep the last value on screen and drop stale responses automatically.
	const store = new Map<number, Block>();
	const revision = ref(0);

	const fetched = defer(combine({ start, take }), DELAY)
		.derive(async ({ start, take }) => {
			if (take === 0) return null;
			const to = tip.height - start; // highest height in the window
			const blocks = await api.fetch("GET /v1/block?to=:to&take=:take", { params: { search: { to, take } } });
			return { to, blocks };
		})
		.derive((promise) => awaited(promise))
		.pipe(unroll);

	// --- rows ------------------------------------------------------------
	// A pool that only grows: a slot is a stable <li> whose content follows a per-slot height
	// signal, so nodes are never torn down and rebuilt — they swap a card in or out by height.
	// While the fetch is owed, a slot is simply empty and aria-busy (its placeholder is ::before).
	type Slot = { item: Builder<HTMLLIElement>; height: Sync.Ref<number> };
	const slots: Slot[] = [];
	const list = ol().ariaLabel("Blocks, newest first");

	const createSlot = (): Slot => {
		const height = ref(-1);
		const block = combine({ height, revision }).derive(({ height }) => store.get(height) ?? null);
		const item = li()
			.ariaSetSize(`${total}`)
			.ariaBusy(block.derive((b) => (b ? null : "true")))
			.$bind(useReplaceChildren(block.derive((block) => (block ? BlockWidget(block) : ""))));
		const slot: Slot = { item, height };
		slots.push(slot);
		list.append$(item);
		list.append$(probe);
		return slot;
	};

	const paint = (start: number, take: number) => {
		while (slots.length < take) createSlot();
		for (let i = 0; i < slots.length; i++) {
			const slot = slots[i]!;
			const active = i < take;
			slot.item.hidden(!active);
			if (!active) continue;
			slot.height.set(tip.height - (start + i));
			slot.item.ariaPosInSet(`${start + i + 1}`);
		}
	};

	const onFetched = (result: { to: number; blocks: Block[] } | null) => {
		if (!result) return;
		const { to, blocks } = result;
		// Response ascends by height, highest last; map each entry to its absolute height.
		for (let i = 0; i < blocks.length; i++) {
			const block = blocks[i];
			if (block) store.set(to - (blocks.length - 1) + i, block);
		}
		while (store.size > CACHE) store.delete(store.keys().next().value!);
		revision.set(revision.val + 1);
	};

	// --- native scroll window --------------------------------------------
	// The spacer owns the (windowed) scroll height; the sticky rows ride over it. Plain numbers
	// here — they're read and written every scroll frame, no reactivity needed.
	const spacer = div();
	let slackTop = 0;
	let slackBottom = 0;
	let anchor = 0;
	let restTop = 0;
	let syncing = false;

	// Re-anchor the native scroll window around a target position: size the spacer to hold SLACK
	// of travel each side (clamped near the ends), then park scrollTop in the middle. Writing
	// scrollTop would fire onScroll, so we gate it with `syncing`.
	const layout = (target: number) => {
		const rs = rowSize.val;
		if (rs <= 0) return;
		const max = maxFirst.val;
		const clamped = Math.min(max, Math.max(0, target));
		slackTop = Math.min(SLACK, clamped * rs);
		slackBottom = Math.min(SLACK, (max - clamped) * rs);
		anchor = clamped;
		restTop = slackTop;
		spacer.$node.style.setProperty("block-size", `${slackTop + viewport.val + slackBottom}px`);
		syncing = true;
		self.$node.scrollTop = restTop;
		syncing = false;
		position.set(clamped);
	};

	const onScroll = () => {
		if (syncing || rowSize.val <= 0) return;
		const drift = self.$node.scrollTop - restTop;
		const next = Math.min(maxFirst.val, Math.max(0, anchor + drift / rowSize.val));
		// Near the edge of the windowed travel: recentre so we never run out of native scroll.
		if (drift < -slackTop * RECENTER_AT || drift > slackBottom * RECENTER_AT) layout(next);
		else position.set(next);
	};

	// Off-axis measuring row: gives us the row pitch without rendering a card.
	const probe = li({ class: "probe" }).ariaHidden("true")
		.$bind(useMeasureBlockSize(rowSize))
		.append$(BlockWidget(tip));
	list.append$(probe);

	const self = section({ class: "surface" })
		.ariaLabel("Block timeline")
		.tabIndex(0) // native scroll is keyboard-driven; this makes the region focusable
		.$bind(ChainTimelineStyle.useScope())
		.$bind(useMeasureBlockSize(viewport, "content-box"))
		.$bind(useStyleProperty("--row-size", rowSize.derive((size) => `${size}px`)))
		.$bind((host) => {
			const aborter = new AbortController();
			host.addEventListener("scroll", onScroll, { passive: true, signal: aborter.signal });

			const unfollows = [
				// row set rebuilds only on a page shift / resize
				combine({ start, take }).follow(({ start, take }) => paint(start, take), true),
				// the transform slides every frame
				offset.follow((value) => (list.$node.style.transform = `translateY(${value}px)`), true),
				// filled blocks land in the store
				fetched.follow(onFetched, true),
				// (re)size and recentre the native scroll window when the row size or viewport changes
				combine({ rowSize, viewport }).follow(() => layout(position.val), true),
			];

			return () => {
				aborter.abort();
				for (const unfollow of unfollows) unfollow();
			};
		});

	return self.append$(
		spacer.append$(
			list,
			ChainScrollbar({
				value: position,
				max: maxFirst,
				tip: tip.height,
				onScrub: layout,
			}),
		),
	);
}

const ChainTimelineStyle = css`
	:scope {
		--row-gap: 0.375em;
		--row-radius: 0.5em;
		--row-placeholder: color-mix(in srgb, currentcolor 4%, transparent);

		display: block flow;
		container-type: size;
		overflow-block: auto; /* the browser owns the scroll */
		overflow-anchor: none; /* don't let scroll anchoring fight our transform */
		overscroll-behavior: contain;
		scrollbar-width: none; /* native bar hidden; the custom rail stands in for it */
	}

	:scope::-webkit-scrollbar {
		display: none;
	}

	:scope:focus-visible {
		outline: 0.125em solid color-mix(in srgb, currentcolor 50%, transparent);
		outline-offset: -0.125em;
	}

	/* The spacer owns the (windowed) scroll height and is the containing block for the rows. */
	:scope > div {
		position: relative;
	}

	/* Off-axis measuring row: laid out (so it has a size) but hidden and inert. Its border-box is
	   one whole row pitch, gap included, so a single measurement is enough. */
	.probe {
		visibility: hidden;
		pointer-events: none;
		user-select: none;
		block-size: max-content;
	}

	/* Sticky, zero-height grid pinned to the viewport top: rows flow out of it and are carried
	   into place by a transform, so nothing waits on a card to know how tall a row is. The gap is
	   baked into each row (not a grid row-gap) so the pitch is exactly one --row-size. */
	ol {
		display: block grid;
		grid-auto-rows: var(--row-size);
		position: sticky;
		inset-block-start: 0;
		block-size: 0;
		margin-block: 0;
		padding-inline-start: .5em;
		padding-inline-end: 1.25em;
		list-style: none;
		will-change: transform;
	}

	li {
		position: relative;
		box-sizing: border-box;
		block-size: var(--row-size);
		padding-block: var(--row-gap);
		overflow: clip; /* a card outgrowing its track is a bug, not a reflow */
	}

	li[hidden] {
		display: none;
	}

	/* The card the row fills with. Fresh node on every fill, so the entrance plays once per
	   reveal and never while a row is just sliding past as a placeholder. */
	li > * {
		display: block flow;
		block-size: 100%;
		animation: chain-row-reveal 560ms ease-out;
	}

	@keyframes chain-row-reveal {
		from {
			opacity: 0;
		}
		to {
			opacity: 1;
		}
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

	@media (prefers-reduced-motion: reduce) {
		li > * {
			animation: none;
		}

		li::before {
			transition: none;
		}
	}
`;
