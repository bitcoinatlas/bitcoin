import { css } from "~/app/frontend/utils/css.ts";

// cards linear-gradient(180deg, var(--panel) 0%, var(--panel-2) 100%);

export const GlobalStyle = css`
	:root {
		--base: hsl(240, 12%, 11%);
		--pop: hsl(0, 0%, 96%);
		--surface: color-mix(in srgb, var(--base), currentcolor 8%);

		--accent-base: hsl(33, 83%, 50%);
		--accent-pop: hsl(33, 50%, 98%);
		--accent-surface: color-mix(in srgb, var(--accent-base), currentcolor 8%);

		--layout-base: hsl(240, 12%, 12%);
		--layout-pop: hsl(0, 0%, 96%);
		--layout-surface: color-mix(in srgb, var(--layout-base), currentcolor 8%);

		--muted-min: color-mix(in srgb, currentcolor, transparent 35%);
		--muted-max: color-mix(in srgb, currentcolor, transparent 88%);

		--border: var(--muted-max);
		--radius-min: 0.35em;
		--radius-max: 0.75em;
		--layout-gap: 0.625em;

		--text-small-3: 0.75em;
		--text-small-2: 0.8125em;
		--text-small-1: 0.875em;
		--text: 1em;
		--text-large-1: 1.125em;

		--weight-regular: 400;
		--weight-medium: 600;
		--weight-bold: 700;
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
		background-image: radial-gradient(1200px 800px at 80% -10%, var(--base) 0%, var(--layout-base) 55%) fixed;
		color: var(--pop);
	}

	a {
		font-weight: var(--weight-bold);
		text-decoration: none;
		&:hover {
			text-decoration: underline;
		}
	}
`;
