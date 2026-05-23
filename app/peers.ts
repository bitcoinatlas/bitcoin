import { type Codec } from "@nomadshiba/codec";
import { PingMessage, PongMessage } from "~/lib/peer/messages/PingPong.ts";
import { VerackMessage } from "~/lib/peer/messages/Verack.ts";
import { VersionMessage, VersionPayload } from "~/lib/peer/messages/Version.ts";
import { Peer, type PeerMessage, type PeerMessageEvent } from "~/lib/peer/Peer.ts";

const USER_AGENT = "/BitcoinAtlas:0.0.1/";
const PROTOCOL_VERSION = 70015;
const SERVICES = 0n;

// ---- state ----

const connected = new Map<string, Peer>(); // "host:port" -> Peer
const known = new Set<string>(); // "host:port"
const failed = new Map<string, { count: number; lastFailed: number }>();
const messageHooks = new Set<(peer: Peer, msg: PeerMessageEvent) => void>();

// ---- internal helpers ----

function key(host: string, port: number): string {
	return `${host}:${port}`;
}

function parseKey(k: string): { host: string; port: number } {
	const i = k.lastIndexOf(":");
	return { host: k.slice(0, i), port: parseInt(k.slice(i + 1)) };
}

function isValidHost(host: string): boolean {
	if (!host || host.endsWith(".onion")) return false;

	if (/^(\d{1,3}\.){3}\d{1,3}$/.test(host)) {
		return host.split(".").map(Number).every((o) => o >= 0 && o <= 255);
	}

	if (host.includes(":")) {
		const dcs = (host.match(/::/g) ?? []).length;
		if (dcs > 1) return false;
		const parts = host.split("::");
		if (parts.length > 2) return false;
		for (const part of parts) {
			if (!part) continue;
			for (const group of part.split(":")) {
				if (!/^[0-9a-fA-F]{1,4}$/.test(group)) return false;
			}
		}
		if (dcs === 0 && host.split(":").length !== 8) return false;
		return true;
	}

	return false;
}

// ---- public API ----

/** All currently connected peers. */
export function peers(): Peer[] {
	return [...connected.values()];
}

/** All known peer addresses (connected or not). */
export function knownPeers(): Array<{ host: string; port: number }> {
	return [...known].map(parseKey);
}

/** Add an address to the known set. Returns false if invalid or already known. */
export function addKnownPeer(host: string, port: number): boolean {
	if (port === 0 || port > 65535) return false;
	if (!isValidHost(host)) return false;
	const k = key(host, port);
	if (known.has(k)) return false;
	known.add(k);
	return true;
}

/** Connect to a peer. Returns the Peer, or null on failure. */
export async function addPeer(host: string, port: number, magic: Uint8Array): Promise<Peer | null> {
	const k = key(host, port);
	const existing = connected.get(k);
	if (existing) return existing;

	const peer = new Peer(host, port, magic);

	peer.onDisconnect((reason) => {
		connected.delete(k);
		if (reason.type !== "manual") {
			const f = failed.get(k);
			if (f) {
				f.count++;
				f.lastFailed = Date.now();
			} else failed.set(k, { count: 1, lastFailed: Date.now() });
		}
	});

	for (const hook of messageHooks) {
		peer.onMessage((msg) => hook(peer, msg));
	}

	try {
		await peer.connect();
		await handshake(peer);
		connected.set(k, peer);
		failed.delete(k);
		return peer;
	} catch (e) {
		const f = failed.get(k);
		if (f) {
			f.count++;
			f.lastFailed = Date.now();
		} else failed.set(k, { count: 1, lastFailed: Date.now() });
		console.error(`[peers] failed to connect ${k}:`, e);
		return null;
	}
}

/** Disconnect and remove a peer. */
export function removePeer(peer: Peer): void {
	peer.disconnect();
	connected.delete(key(peer.host, peer.port));
}

/** Mark a peer as recently failed (e.g. after handshake failure). */
export function markFailed(host: string, port: number): void {
	const k = key(host, port);
	const f = failed.get(k);
	if (f) {
		f.count++;
		f.lastFailed = Date.now();
	} else failed.set(k, { count: 1, lastFailed: Date.now() });
}

/** Expire failed peers older than retryAfterMs. */
export function expireFailed(retryAfterMs: number): void {
	const now = Date.now();
	for (const [k, f] of failed) {
		if (now - f.lastFailed > retryAfterMs) failed.delete(k);
	}
}

/** Available peers: known but not connected or recently failed. */
export function availablePeers(): Array<{ host: string; port: number }> {
	const unavailable = new Set([...connected.keys(), ...failed.keys()]);
	return [...known].filter((k) => !unavailable.has(k)).map(parseKey);
}

/** Register a message listener across all current and future peers. */
export function onMessage(listener: (peer: Peer, msg: PeerMessageEvent) => void): () => void {
	const unlisteners: Array<() => void> = [];
	for (const peer of connected.values()) {
		unlisteners.push(peer.onMessage((msg) => listener(peer, msg)));
	}
	messageHooks.add(listener);
	return () => {
		messageHooks.delete(listener);
		for (const u of unlisteners) u();
	};
}

/** Send to all connected peers, ignoring individual failures. */
export async function broadcast<T extends Codec>(
	message: PeerMessage<T>,
	data: Codec.InferInput<T>,
): Promise<void> {
	await Promise.allSettled(
		[...connected.values()].map((p) => p.send(message, data)),
	);
}

/** Resolve a DNS seed hostname, adding discovered IPs to the known set. */
export async function addPeersFromDNS(seedHost: string, port: number): Promise<number> {
	let added = 0;
	try {
		const ips = await Deno.resolveDns(seedHost, "A");
		for (const ip of ips) {
			if (addKnownPeer(ip, port)) added++;
		}
	} catch (e) {
		console.error(`[peers] DNS seed ${seedHost} failed:`, e);
	}
	return added;
}

/**
 * Initiate outbound handshake:
 * 1. Send our version
 * 2. Wait for verack from remote
 * Also installs a one-shot handler to reply verack if the remote sends version first.
 */
export async function handshake(peer: Peer): Promise<void> {
	// Handle inbound version — reply with verack
	const unlisten = peer.onMessage((msg) => {
		if (msg.command !== "version") return;
		unlisten();
		peer.send(VerackMessage, null).catch(() => {});
	});

	const localIp = "0.0.0.0";
	const localPort = 0;

	const versionPayload: VersionPayload = {
		version: PROTOCOL_VERSION,
		services: SERVICES,
		timestamp: BigInt(Math.floor(Date.now() / 1000)),
		recvServices: SERVICES,
		recvIP: peer.host,
		recvPort: peer.port,
		transServices: SERVICES,
		transIP: localIp,
		transPort: localPort,
		nonce: BigInt(Math.floor(Math.random() * Number.MAX_SAFE_INTEGER)),
		userAgent: USER_AGENT,
		startHeight: 0,
		relay: false,
	};

	await peer.send(VersionMessage, versionPayload);
	console.log(`[handshake] sent version → ${peer.host}:${peer.port}`);

	await peer.expect(VerackMessage);
	console.log(`[handshake] complete ✓ ${peer.host}:${peer.port}`);

	// Reply to ping with pong echoing the nonce, for the lifetime of the peer.
	peer.onMessage((msg) => {
		if (msg.command !== PingMessage.command) return;
		const [nonce] = PingMessage.codec.decode(msg.payload);
		peer.send(PongMessage, nonce).catch(() => {});
	});
}
