import { Peer } from "./Peer.ts";
import { PeerMessageHandler } from "./PeerMessageHandler.ts";
import { PeerSeed } from "./PeerSeed.ts";
import { getAddr } from "./handlers/GetAddrHandler.ts";
import { ping } from "./handlers/PingHandler.ts";
import { handshake } from "./handlers/VersionHandler.ts";
import { Version } from "./messages/Version.ts";

export declare namespace PeerManager {
	export type Init = {
		magic: Uint8Array;
		version: Version;
		maxConnections: number;
		seeds: PeerSeed[];
		handlers: PeerMessageHandler<unknown>[];
	};
}

type FailedPeerInfo = {
	count: number;
	lastFailed: number;
};

export class PeerManager {
	private readonly magic: Uint8Array;
	private readonly version: Version;
	private readonly maxConnections: number;
	private readonly seeds: PeerSeed[];
	private readonly handlers: Map<string, PeerMessageHandler<unknown>>;

	private readonly connectedPeers = new Map<string, Peer>(); // host:port -> Peer
	private readonly knownPeers = new Set<string>(); // host:port - discovered peer addresses
	private readonly failedPeers = new Map<string, FailedPeerInfo>(); // host:port -> failure info

	private readonly lastAddressRequest = new WeakMap<Peer, number>(); // host:port -> timestamp of last getaddr request
	private readonly lastHeartbeat = new WeakMap<Peer, number>(); // host:port -> timestamp of last heartbeat sent

	constructor(init: PeerManager.Init) {
		this.magic = init.magic;
		this.version = init.version;
		this.maxConnections = init.maxConnections;
		this.seeds = init.seeds;
		this.handlers = new Map(init.handlers.map((h) => [h.message.command, h]));
	}

	addKnownPeer(address: Peer.Address): boolean {
		// Filter out invalid ports
		if (address.port === 0 || address.port > 65535) return false;

		// Filter out invalid addresses
		if (!this.isValidPeerAddress(address.host)) return false;

		const key = `${address.host}:${address.port}`;

		// Check if already known
		if (this.knownPeers.has(key)) return false;

		this.knownPeers.add(key);
		return true;
	}

	getKnownPeers(): Array<Peer.Address> {
		return this.knownPeers.values().map((key) => {
			const [host, port] = key.split(":");
			return { host: host!, port: parseInt(port!) };
		}).toArray();
	}

	peers(): Iterable<Peer> {
		return this.connectedPeers.values();
	}

	randomPeer(triedPeers?: Set<Peer>): Peer | undefined {
		if (this.connectedPeers.size === 0) return undefined;
		const available = new Set(this.connectedPeers.values());
		if (triedPeers) {
			for (const peer of triedPeers) {
				available.delete(peer);
			}
		}
		if (available.size === 0) return undefined;
		const index = Math.floor(Math.random() * available.size);
		return available.values().drop(index).next().value;
	}

	disconnectAll(): void {
		for (const peer of this.connectedPeers.values()) {
			peer.disconnect();
		}
	}

	async broadcast<T>(message: Peer.Message<T>, data: T): Promise<void> {
		const promises = this.connectedPeers.values().map((peer) =>
			peer.send(message, data).catch((e) => {
				peer.logError("Failed to broadcast:", e);
			})
		);
		await Promise.allSettled(promises);
	}

	async maintainConnections(): Promise<void> {
		for (const peer of this.connectedPeers.values()) {
			const lastHeartbeat = this.lastHeartbeat.get(peer) || 0;
			const now = Date.now();
			const heartbeatInterval = 2 * 60 * 1000; // 2 minutes

			if (now - lastHeartbeat > heartbeatInterval) {
				peer.log("💓 Sending heartbeat ping");
				try {
					await ping(peer);
					this.lastHeartbeat.set(peer, now);
				} catch (e) {
					peer.logError("💓 Heartbeat ping failed:", e);
					peer.disconnect({ type: "heartbeat_timeout" });
				}
			}
		}

		if (this.connectedPeers.size >= this.maxConnections) return;

		const needed = this.maxConnections - this.connectedPeers.size;

		const now = Date.now();
		const failedPeerRetryTime = 5 * 60 * 1000; // 5 minutes
		for (const [key, info] of this.failedPeers) {
			if (now - info.lastFailed > failedPeerRetryTime) {
				this.failedPeers.delete(key);
			}
		}

		const connectedAddrs = new Set(this.connectedPeers.keys());

		const failedAddrsSet = new Set(this.failedPeers.keys());
		const unavailableAddrs = failedAddrsSet.union(connectedAddrs);
		const availableAddrs = this.knownPeers.difference(unavailableAddrs);

		const stats = {
			known: this.knownPeers.size,
			failed: this.failedPeers.size,
			connected: this.connectedPeers.size,
			available: availableAddrs.size,
			needed,
			target: this.maxConnections,
		};

		console.log(`Peer stats:`, stats);

		if (availableAddrs.size > 0) {
			const addresses = availableAddrs.values().map((key) => {
				const [host, port] = key.split(":") as [string, string];
				return { host, port: parseInt(port) };
			});

			const addressesToTry = addresses.take(needed);
			const results = await Promise.allSettled(addressesToTry.map((address) => this.connectPeer(address)));

			const successful = results.filter((r) => r.status === "fulfilled" && r.value !== null).length;
			if (successful > 0) {
				console.log(
					`Successfully connected to ${successful} peers, now have ${this.connectedPeers.size}/${this.maxConnections} peers`,
				);
			}

			if (this.connectedPeers.size >= this.maxConnections) {
				console.log(`Reached target of ${this.maxConnections} connections`);
			}
		}

		if (
			availableAddrs.size === 0 &&
			this.connectedPeers.size > 0 &&
			this.connectedPeers.size < this.maxConnections
		) {
			let fetched = false;
			for (const peer of this.connectedPeers.values()) {
				fetched = await this.discoverFromPeer(peer);
			}
			if (fetched) {
				console.log(`Discovered new peer addresses from connected peers`);
			} else {
				console.log(`No peers available to fetch addresses from (all recently asked)`);
			}
		}

		if (
			this.connectedPeers.size < this.maxConnections &&
			availableAddrs.size === 0
		) {
			for (const seed of this.seeds) {
				await this.discoverFromDNS(seed);
			}
		}
	}

	private async connectPeer(address: Peer.Address): Promise<Peer | null> {
		const key = `${address.host}:${address.port}`;
		let peer = this.connectedPeers.get(key);
		if (!peer) {
			const newPeer = new Peer(address, this.magic);
			newPeer.onDisconnect((reason) => {
				const key = `${newPeer.remoteHost}:${newPeer.remotePort}`;
				this.connectedPeers.delete(key);

				const reasonType = reason.type;

				if (reasonType !== "manual") {
					newPeer.logWarn(`Disconnected: ${reasonType}`);
					const existing = this.failedPeers.get(key);
					if (existing) {
						existing.count++;
						existing.lastFailed = Date.now();
					} else {
						this.failedPeers.set(key, { count: 1, lastFailed: Date.now() });
					}
				}
			});

			newPeer.listen((msg) => {
				this.lastHeartbeat.set(newPeer, Date.now());
				const handler = this.handlers.get(msg.command);
				if (!handler) return;
				handler.handle({
					peer: newPeer,
					data: handler.message.codec.decode(msg.payload)[0],
					ctx: { peerManager: this },
				});
			});

			try {
				await newPeer.connect();
				await handshake(newPeer, {
					...this.version,
					recvIP: newPeer.remoteIp,
					recvPort: newPeer.remotePort,
					transIP: newPeer.localIp,
					transPort: newPeer.localPort,
				});

				this.connectedPeers.set(key, newPeer);
				this.failedPeers.delete(key);
			} catch (e) {
				const existing = this.failedPeers.get(key);
				if (existing) {
					existing.count++;
					existing.lastFailed = Date.now();
				} else {
					this.failedPeers.set(key, { count: 1, lastFailed: Date.now() });
				}
				newPeer.logError("Failed to connect:", e);
				return null;
			}

			await this.discoverFromPeer(newPeer).catch((e) => {
				newPeer.logError("Failed to request addresses:", e);
			});

			peer = newPeer;
		}

		return peer;
	}

	private isValidPeerAddress(host: string): boolean {
		// Filter out Tor onion addresses (.onion)
		if (host.endsWith(".onion")) return false;

		// Filter out invalid/incomplete addresses
		if (!host || host.length < 7) return false;

		// IPv4 validation (e.g., "192.168.1.1")
		const ipv4Regex = /^(\d{1,3}\.){3}\d{1,3}$/;
		if (ipv4Regex.test(host)) {
			// Validate each octet is 0-255
			const octets = host.split(".").map(Number);
			return octets.every((octet) => octet >= 0 && octet <= 255);
		}

		// IPv6 validation - must have at least 2 colons and proper format
		// Full format: 8 groups of 4 hex digits separated by colons
		// Compressed format: consecutive zeros can be replaced with ::
		if (host.includes(":")) {
			// Must have at least 2 colons for valid IPv6
			const colonCount = (host.match(/:/g) || []).length;
			if (colonCount < 2) return false;

			// Check for valid IPv6 pattern
			// Allow :: for compressed zeros, but only once
			const doubleColonCount = (host.match(/::/g) || []).length;
			if (doubleColonCount > 1) return false;

			// Split and validate groups
			const parts = host.split("::");
			if (parts.length > 2) return false;

			for (const part of parts) {
				if (part === "") continue;
				const groups = part.split(":");
				for (const group of groups) {
					// Each group must be 1-4 hex digits
					if (!/^[0-9a-fA-F]{1,4}$/.test(group)) return false;
				}
			}

			// If no ::, must have exactly 8 groups
			if (doubleColonCount === 0) {
				const groups = host.split(":");
				if (groups.length !== 8) return false;
			}

			return true;
		}

		// Reject anything else (partial IPs, invalid formats, etc.)
		return false;
	}

	private async discoverFromDNS(seed: PeerSeed): Promise<void> {
		const addrs = await Deno.resolveDns(seed.seedHost, "A");
		for (const ip of addrs) {
			if (this.connectedPeers.size >= this.maxConnections) break;
			const address: Peer.Address = { host: ip, port: seed.peerPort };
			this.addKnownPeer(address);
		}
	}

	private async discoverFromPeer(peer: Peer): Promise<boolean> {
		const lastRequest = this.lastAddressRequest.get(peer);
		if (lastRequest && Date.now() - lastRequest < 10 * 60 * 1000) {
			peer.log("📬 Recently requested addresses, skipping");
			return false;
		}
		this.lastAddressRequest.set(peer, Date.now());
		const addrs = await getAddr(peer);

		const now = Date.now();
		const maxAge = 24 * 60 * 60 * 1000;
		let added = 0;
		let stale = 0;
		let rejected = 0;

		for (const addr of addrs.addresses) {
			// Filter out stale addresses
			if (now - addr.timestamp > maxAge) {
				stale++;
				continue;
			}

			if (this.addKnownPeer(addr)) {
				added++;
			} else {
				rejected++;
			}
		}

		peer.log(
			`📬 First connect: ${addrs.addresses.length} addresses: ${added} added, ${stale} stale, ${rejected} rejected`,
		);

		return true;
	}
}
