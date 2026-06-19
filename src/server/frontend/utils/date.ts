const units = [
	["year", 1000 * 60 * 60 * 24 * 365],
	["month", 1000 * 60 * 60 * 24 * 30],
	["week", 1000 * 60 * 60 * 24 * 7],
	["day", 1000 * 60 * 60 * 24],
	["hour", 1000 * 60 * 60],
	["minute", 1000 * 60],
	["second", 1000],
] as const;

export function getRelativeDate(to: Date, from: Date = new Date()) {
	const rtf = new Intl.RelativeTimeFormat("en-US", { numeric: "auto" });

	const diff = to.getTime() - from.getTime();

	for (const [unit, msInUnit] of units) {
		const diffInUnits = diff / msInUnit;
		if (Math.abs(diffInUnits) >= 1) {
			return rtf.format(Math.round(diffInUnits), unit);
		}
	}

	return rtf.format(0, "second"); // fallback: "now"
}
