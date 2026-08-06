export function formatDuration(ms: number): string {
	if (!Number.isFinite(ms) || ms < 0) ms = 0;
	let s = Math.floor(ms / 1000);
	const d = Math.floor(s / 86400);
	s %= 86400;
	const h = Math.floor(s / 3600);
	s %= 3600;
	const m = Math.floor(s / 60);
	s %= 60;

	let out = "";
	if (d) out += `${d}d`;
	if (h) out += `${h}h`;
	if (m) out += `${m}m`;
	if (s || out === "") out += `${s}s`; // always show seconds if nothing else
	return out;
}
