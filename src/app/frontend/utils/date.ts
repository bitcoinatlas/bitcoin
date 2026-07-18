import { DAY, HOUR, MINUTE, MONTH, SECOND, WEEK, YEAR } from "~/constants.ts";

const units = [
	["year", YEAR],
	["month", MONTH],
	["week", WEEK],
	["day", DAY],
	["hour", HOUR],
	["minute", MINUTE],
	["second", SECOND],
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
