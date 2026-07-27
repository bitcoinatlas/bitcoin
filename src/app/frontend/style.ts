import { css } from "~/app/frontend/utils/dom/css.ts";

export const GlobalStyle = css`
	:root {
		--base: hsl(240, 12%, 11%);
		--pop: hsl(0, 0%, 96%);

		--accent-base: hsl(33, 83%, 50%);
		--accent-pop: hsl(36, 46%, 98%);

		--mute-min: 35%;
		--mute-max: 88%;

		--radius-min: 0.35em;
		--radius-max: 0.75em;

		--panel-radius: var(--radius-max);
		--panel-surface: linear-gradient(to bottom, color-mix(in srgb, var(--base), currentcolor 2%) 0%, var(--base) 100%);
		--panel-shadow: inset 0 0.06em 0 0 color-mix(in srgb, currentcolor 9%, transparent), inset 0 -0.14em 10px -0.25em color-mix(in srgb, black 45%, transparent);
	}

	:root {
		color-scheme: dark;
		font-family: ui-monospace, "SF Mono", "JetBrains Mono", "Cascadia Code", Menlo, Consolas, monospace;
		line-height: 1.4;
		font-size: 1rem;
		accent-color: var(--accent-base);

		-webkit-font-smoothing: antialiased;
		-moz-osx-font-smoothing: grayscale;
	}

	*,
	*::before,
	*::after {
		box-sizing: border-box;
	}

	* {
		margin: 0;
	}

	html {
		container-type: inline-size;
	}

	body {
		background-color: var(--base);
		color: var(--pop);
	}

	a {
		font-weight: bolder;
		text-decoration: none;
		&:hover {
			text-decoration: underline;
		}
	}

	ol,
	ul {
		list-style: none;
		padding: 0;
	}
`;
