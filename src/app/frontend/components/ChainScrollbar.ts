import { combine, ref, Sync, tags } from "@purifyjs/core";
import { css } from "~/app/frontend/utils/css.ts";
import { HALVING_BLOCKS } from "~/constants.ts";

// Ruler gradations, in blocks: halvings are the major bands, a twentieth of a halving the minor
// ticks (difficulty epochs are too dense and drift, so they're deferred to a zoom level).
const MINOR = HALVING_BLOCKS * 0.05;
const MAJOR = HALVING_BLOCKS;

const { div, span, input } = tags;

// A purely VISUAL scrollbar: aria-hidden and out of the tab order, because the section it rides
// on is a real, natively scrollable region and is already the accessible surface (keyboard,
// screen reader, touch all go through the native scroll). This is a sighted-pointer affordance
// only — a draggable thumb, a halving/epoch ruler and a height readout. It reflects `value` and,
// when dragged, asks the timeline to re-anchor the native scroll through `onScrub`.
export function ChainScrollbar(props: {
	value: Sync.Ref<number>; // position, fractional rows; reflected onto the thumb
	max: Sync<number>; // maxFirst — the deepest the viewport can scroll
	tip: number; // tip height, for the ruler fractions and labels
	onScrub: (value: number) => void; // user dragged the thumb
}) {
	// Fraction of the axis the thumb sits at, for the readout that rides it.
	const at = combine({ value: props.value, max: props.max })
		.derive(({ value, max }) => (max > 0 ? Math.min(1, Math.max(0, value / max)) : 0));
	// Block height under the thumb, for the readout.
	const height = props.value.derive((value) => props.tip - Math.round(value));

	// Surface the ruler + readout while the value is moving (scroll or scrub), then fade — so
	// scrolling gets the same affordance as hovering, without a hover.
	const active = ref(false);
	let idle: ReturnType<typeof setTimeout>;
	const activate = () => {
		active.set(true);
		clearTimeout(idle);
		idle = setTimeout(() => active.set(false), 700);
	};

	const slider = input({ type: "range" })
		.tabIndex(-1) // aria-hidden, so keep it out of the tab order too
		.min("0")
		.max(props.max.derive((value) => String(Math.max(0, Math.ceil(value)))))
		.step("any")
		.$bind((element) => {
			const aborter = new AbortController();
			element.addEventListener("input", () => props.onScrub(element.valueAsNumber), { signal: aborter.signal });
			// Reflect scroll-driven changes back onto the thumb, but never fight a value the slider
			// already holds (mid-drag it's the source). Any real change surfaces the ruler; the
			// immediate bind call doesn't.
			let first = true;
			const unfollow = props.value.follow((value) => {
				if (element.valueAsNumber !== value) element.valueAsNumber = value;
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
		.ariaHidden("true")
		.$bind(ChainScrollbarStyle.useScope())
		.$bind((element) => active.follow((on) => element.classList.toggle("active", on), true))
		.append$(
			slider,
			div({ class: "ruler" })
				.$bind((element) => at.follow((value) => element.style.setProperty("--at", String(value)), true))
				.append$(div({ class: "readout" }).append$(span().textContent(height.derive((h) => h.toLocaleString())))),
		);
}

const ChainScrollbarStyle = css`
	:scope {
		--thumb-inline-size: 1.25em;
		--thumb-block-size: .75em;
		--thumb-wall-gap: 0.1em;
		--track-pad: calc(var(--thumb-block-size) * 0.5);

		position: sticky;
		inset-block-start: 0;
		margin-inline-start: auto;
		inline-size: 1.25em;
		block-size: 100cqb;
	}

	/* The native range: fills the box, runs vertically, its own track invisible so the ruler
	   shows through. It keeps the thumb and pointer behaviour; keyboard/a11y live on the native
	   scroll region, not here. */
	[type="range"] {
		position: absolute;
		writing-mode: vertical-lr;
		inset-inline: calc(var(--track-pad) - calc(var(--thumb-block-size) * 0.5));
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
		border-radius: .5em;
		background-color: currentcolor;
		opacity: 0.55;
		box-shadow: 0 0.0625em 4px color-mix(in srgb, currentcolor 35%, transparent);
		transition: opacity 150ms ease;
	}

	[type="range"]::-moz-range-thumb {
		block-size: var(--thumb-inline-size);
		inline-size: var(--thumb-block-size);
		border: none;
		border-radius: .5em;
		background-color: currentcolor;
		opacity: 0.55;
		box-shadow: 0 0.0625em 4px color-mix(in srgb, currentcolor 35%, transparent);
		transition: opacity 150ms ease;
	}

	[type="range"]:hover::-webkit-slider-thumb,
	:scope.active [type="range"]::-webkit-slider-thumb {
		opacity: 0.9;
	}

	[type="range"]:hover::-moz-range-thumb,
	:scope.active [type="range"]::-moz-range-thumb {
		opacity: 0.9;
	}

	/* The value axis, a box inset by the end gutter, so 100% here *is* the travel and the
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
			color-mix(in srgb, currentcolor 60%, transparent) 0 0.0625em,
			transparent 0.0625em
		),
			repeating-linear-gradient(
			to top,
			color-mix(in srgb, currentcolor 45%, transparent) 0 0.0625em,
			transparent 0.0625em var(--major)
		),
			repeating-linear-gradient(
			to top,
			color-mix(in srgb, currentcolor 20%, transparent) 0 0.0625em,
			transparent 0.0625em var(--minor)
		);
		background-repeat: no-repeat;
		background-size: 0.9em 100%, 0.9em 100%, 0.4em 100%;
		background-position-x: 100%;
	}

	.ruler .mark {
		display: block flow;
		position: absolute;
		inset-inline-end: calc(var(--thumb-inline-size) + 0.15em);
		inset-block-start: calc(100% * var(--mark-at, 0));
		translate: 0 -50%;
		line-height: 1;
		white-space: nowrap;
		font-variant-numeric: tabular-nums;
		opacity: 0.3;
		transition: opacity 150ms ease;

		span {
			font-size: 0.65em;
		}
	}

	:scope:is(:hover, .active) .ruler .mark {
		opacity: 0.65;
	}

	.readout {
		display: block flow;
		position: absolute;
		inset-block-start: calc(100% * var(--at, 0));
		inset-inline-end: calc(var(--thumb-inline-size) + 0.15em);
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
			font-size: 0.85em;
		}
	}

	:scope:is(:hover, .active) .readout {
		opacity: 1;
	}
`;
