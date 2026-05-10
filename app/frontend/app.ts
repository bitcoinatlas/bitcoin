type BlockHeader = {
	height: number;
	hash: string;
	prevHash: string;
	timestamp: number;
	bits: number;
	nonce: number;
	version: number;
};

type HeadersResponse = {
	headers: BlockHeader[];
	tip: number;
};

const PAGE_SIZE = 40;
let currentTip = 0;
let lowestLoaded = Infinity;
let loading = false;
let initialized = false;

const list = document.getElementById("blocks") as HTMLElement;
const status = document.getElementById("status") as HTMLElement;
const tipEl = document.getElementById("tip") as HTMLElement;

function toHex(hash: string): string {
	return hash;
}

function shortHex(hash: string): string {
	return hash.slice(0, 8) + "…" + hash.slice(-8);
}

function formatTime(ts: number): string {
	const d = new Date(ts * 1000);
	return d.toUTCString();
}

function difficultyFromBits(bits: number): string {
	// Approximate difficulty: difficulty_1_target / current_target
	const exp = bits >>> 24;
	const mant = bits & 0xffffff;
	// just show bits as hex for now, full diff calc is complex
	return "0x" + bits.toString(16).padStart(8, "0");
}

function renderBlock(h: BlockHeader): HTMLElement {
	const el = document.createElement("div");
	el.className = "block";
	el.dataset["height"] = String(h.height);

	el.innerHTML = `
		<div class="block-header" data-height="${h.height}">
			<div class="block-left">
				<span class="height">#${h.height.toLocaleString()}</span>
				<span class="time">${formatTime(h.timestamp)}</span>
			</div>
			<div class="block-right">
				<span class="hash-short" title="${h.hash}">${shortHex(h.hash)}</span>
				<span class="chevron">▶</span>
			</div>
		</div>
		<div class="block-detail" hidden>
			<table>
				<tr><td class="field">Height</td><td class="value">${h.height.toLocaleString()}</td></tr>
				<tr><td class="field">Hash</td><td class="value hash-full">${toHex(h.hash)}</td></tr>
				<tr><td class="field">Prev Hash</td><td class="value hash-full">${toHex(h.prevHash)}</td></tr>
				<tr><td class="field">Time</td><td class="value">${formatTime(h.timestamp)} <span class="dim">(${h.timestamp})</span></td></tr>
				<tr><td class="field">Version</td><td class="value">0x${h.version.toString(16)}</td></tr>
				<tr><td class="field">Bits</td><td class="value">${difficultyFromBits(h.bits)}</td></tr>
				<tr><td class="field">Nonce</td><td class="value">${h.nonce.toLocaleString()}</td></tr>
			</table>
		</div>
	`;

	const header = el.querySelector(".block-header") as HTMLElement;
	const detail = el.querySelector(".block-detail") as HTMLElement;
	const chevron = el.querySelector(".chevron") as HTMLElement;

	header.addEventListener("click", () => {
		const open = !detail.hidden;
		detail.hidden = open;
		chevron.textContent = open ? "▶" : "▼";
		el.classList.toggle("open", !open);
	});

	return el;
}

async function fetchHeaders(fromHeight: number): Promise<HeadersResponse> {
	const res = await fetch(`/api/headers?from=${fromHeight}&limit=${PAGE_SIZE}`);
	if (!res.ok) throw new Error(`HTTP ${res.status}`);
	return res.json() as Promise<HeadersResponse>;
}

async function loadPage(fromHeight: number) {
	if (loading) return;
	loading = true;
	status.textContent = "Loading…";

	try {
		const data = await fetchHeaders(fromHeight);
		currentTip = data.tip;
		tipEl.textContent = `Tip: #${currentTip.toLocaleString()}`;

		for (const h of data.headers) {
			list.appendChild(renderBlock(h));
			if (h.height < lowestLoaded) lowestLoaded = h.height;
		}

		status.textContent = data.headers.length === 0 ? "Genesis reached" : "";
	} catch (e) {
		status.textContent = `Error: ${e}`;
	} finally {
		loading = false;
	}
}

window.addEventListener("scroll", () => {
	const nearBottom = window.innerHeight + window.scrollY >= document.body.offsetHeight - 400;
	if (!nearBottom || loading || lowestLoaded <= 0) return;
	loadPage(lowestLoaded - 1);
});

async function init() {
	if (initialized) return;
	initialized = true;
	try {
		const res = await fetch("/api/tip");
		if (!res.ok) throw new Error(`HTTP ${res.status}`);
		const { height } = await res.json() as { height: number };
		await loadPage(height);
	} catch (e) {
		status.textContent = `Error: ${e}`;
	}
}

init();
