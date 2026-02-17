import { sha256 } from "@noble/hashes/sha2";

// --- small fast utils ---
const ASCII = new TextDecoder("ascii");
const ASCII_ENC = new TextEncoder();

function u32le(a: Uint8Array, off: number): number {
	return (a[off]! | (a[off + 1]! << 8) | (a[off + 2]! << 16) | (a[off + 3]! << 24)) >>> 0;
}
function putU32le(a: Uint8Array, off: number, v: number) {
	a[off] = v & 0xff;
	a[off + 1] = (v >>> 8) & 0xff;
	a[off + 2] = (v >>> 16) & 0xff;
	a[off + 3] = (v >>> 24) & 0xff;
}
function trimCmd(buf: Uint8Array): string {
	// avoid regex; find last non-NUL
	let end = buf.length;
	while (end > 0 && buf[end - 1] === 0) end--;
	return ASCII.decode(buf.subarray(0, end));
}

// --- lock-free, growable byte queue (single producer/consumer) ---
class ByteQueue {
	private buf: Uint8Array;
	private r = 0; // read offset
	private w = 0; // write offset

	constructor(initial = 32 * 1024) {
		this.buf = new Uint8Array(initial);
	}

	get length(): number {
		return this.w - this.r;
	}

	/** Ensure capacity for `need` more bytes to be written. */
	private ensure(need: number) {
		const freeTail = this.buf.length - this.w;
		if (freeTail >= need) return;

		// compact if it helps
		const used = this.length;
		if (this.r > 0) {
			this.buf.copyWithin(0, this.r, this.w);
			this.w = used;
			this.r = 0;
		}
		if (this.buf.length - this.w >= need) return;

		// grow (double until enough)
		let cap = this.buf.length;
		const required = this.w + need;
		while (cap < required) cap <<= 1;
		const nb = new Uint8Array(cap);
		nb.set(this.buf.subarray(0, this.w)); // keep used region [0..w)
		this.buf = nb;
	}

	/** Append `src[0..n)` */
	append(src: Uint8Array, n: number) {
		this.ensure(n);
		this.buf.set(src.subarray(0, n), this.w);
		this.w += n;
	}

	/** Peek a view into [r .. r+len) without copying. */
	peek(len: number): Uint8Array {
		return this.buf.subarray(this.r, this.r + len);
	}

	/** Advance read cursor. */
	consume(n: number) {
		this.r += n;
		if (this.r === this.w) {
			// reset to avoid counter growth
			this.r = 0;
			this.w = 0;
		}
	}

	/** Find magic sequence starting at/after current read cursor. Returns absolute offset or -1. */
	findMagic(magic: Uint8Array): number {
		const m0 = magic[0];
		const limit = this.w - magic.length + 1;
		for (let i = this.r; i < limit; i++) {
			if (this.buf[i] !== m0) continue;
			// small fixed compare; 4 bytes
			if (
				this.buf[i + 1] === magic[1] &&
				this.buf[i + 2] === magic[2] &&
				this.buf[i + 3] === magic[3]
			) return i;
		}
		return -1;
	}

	/** Compare 4 bytes at pos with magic. */
	startsWith(magic: Uint8Array): boolean {
		const i = this.r;
		return (
			this.w - i >= magic.length &&
			this.buf[i] === magic[0] &&
			this.buf[i + 1] === magic[1] &&
			this.buf[i + 2] === magic[2] &&
			this.buf[i + 3] === magic[3]
		);
	}

	/** Get raw backing view (debug/advanced). */
	raw(): { buf: Uint8Array; r: number; w: number } {
		return { buf: this.buf, r: this.r, w: this.w };
	}
}

// ---- Peer types ----
export declare namespace Peer {
	export type Address = {
		host: string;
		port: number;
	};

	export type MessagePayload = {
		command: string;
		payload: Uint8Array; // ephemeral view
	};
	export type Message<T> = {
		command: string; // 1..12 ASCII
		codec: BoundCodec<T>;
	};
	export type Listener = (msg: Peer.MessagePayload) => void;
	export type Unlistener = () => void;

	export type DisconnectReason =
		| { type: "manual" }
		| { type: "read_error"; error: unknown }
		| { type: "connection_closed" }
		| { type: "write_timeout" }
		| { type: "heartbeat_timeout" };

	export type DisconnectCallback = (reason: DisconnectReason) => void;
}

// ---- Peer impl ----
const MAGIC_LEN = 4;
const CMD_LEN = 12;
const HDR_LEN = 24;
const READ_CHUNK = 32 * 1024;

export class Peer {
	#connected = false;
	public get connected(): boolean {
		return this.#connected;
	}

	public readonly remoteHost: string;
	public readonly remotePort: number;

	public get remoteIp(): string {
		const c = this.connection;
		if (!c) throw new Error("Not connected");
		if (c.remoteAddr.transport !== "tcp") throw new Error("Not TCP");
		return c.remoteAddr.hostname;
	}
	public get localIp(): string {
		const c = this.connection;
		if (!c) throw new Error("Not connected");
		if (c.localAddr.transport !== "tcp") throw new Error("Not TCP");
		return c.localAddr.hostname;
	}
	public get localPort(): number {
		const c = this.connection;
		if (!c) throw new Error("Not connected");
		if (c.localAddr.transport !== "tcp") throw new Error("Not TCP");
		return c.localAddr.port;
	}

	public readonly magic: Uint8Array;
	private readonly listeners: Set<Peer.Listener> = new Set();
	private readonly disconnectCallbacks: Set<Peer.DisconnectCallback> = new Set();
	private connection: Deno.Conn | null = null;

	constructor(address: Peer.Address, magic: Uint8Array) {
		if (magic.length !== MAGIC_LEN) throw new Error("magic must be 4 bytes");
		this.remoteHost = address.host;
		this.remotePort = address.port;
		this.magic = magic;
	}

	async connect(): Promise<void> {
		if (this.#connected) return;

		const abort = new AbortController();
		const timer = setTimeout(() => abort.abort(), 5000);
		try {
			this.connection = await Deno.connect({
				hostname: this.remoteHost,
				port: this.remotePort,
				transport: "tcp",
				signal: abort.signal,
			});
		} finally {
			clearTimeout(timer);
		}

		this.#connected = true;

		// detached read loop (errors handled internally)
		void this.readLoop(this.connection);
	}

	disconnect(reason: Peer.DisconnectReason = { type: "manual" }): void {
		if (!this.#connected) return;
		this.#connected = false;

		// Close connection
		if (this.connection) {
			try {
				this.connection.close();
			} catch { /* noop */ }
			this.connection = null;
		}

		// Notify callbacks
		for (const cb of this.disconnectCallbacks) {
			try {
				cb(reason);
			} catch { /* ignore callback errors */ }
		}
	}

	onDisconnect(callback: Peer.DisconnectCallback): Peer.Unlistener {
		this.disconnectCallbacks.add(callback);
		return () => {
			this.disconnectCallbacks.delete(callback);
		};
	}

	async send<T>(message: Peer.Message<T>, data: T, timeoutMs = 10_000): Promise<void> {
		const conn = this.connection;
		if (!this.#connected || !conn) throw new Error("Peer is not connected");

		const cmd = message.command;
		// minimal check; avoid regex
		if (cmd.length < 1 || cmd.length > CMD_LEN) throw new Error("Invalid command len");
		for (let i = 0; i < cmd.length; i++) {
			const c = cmd.charCodeAt(i);
			if (c < 0x20 || c > 0x7e) throw new Error("Command must be printable ASCII");
		}

		const payload = message.codec.encode(data);
		const out = new Uint8Array(HDR_LEN + payload.length);

		// magic
		out[0] = this.magic[0]!;
		out[1] = this.magic[1]!;
		out[2] = this.magic[2]!;
		out[3] = this.magic[3]!;

		// command padded with NULs to 12
		const cmdBytes = ASCII_ENC.encode(cmd);
		out.set(cmdBytes, 4);
		if (cmdBytes.length < CMD_LEN) out.fill(0, 4 + cmdBytes.length, 16);

		// length
		putU32le(out, 16, payload.length);

		// checksum
		const cs = sha256(sha256(payload));
		out[20] = cs[0]!;
		out[21] = cs[1]!;
		out[22] = cs[2]!;
		out[23] = cs[3]!;

		// payload
		out.set(payload, HDR_LEN);

		// Write with timeout
		const abort = new AbortController();
		const timer = setTimeout(() => abort.abort(), timeoutMs);
		try {
			await conn.write(out);
		} catch (e) {
			if (abort.signal.aborted) {
				this.disconnect({ type: "write_timeout" });
				throw new Error(`Write timeout after ${timeoutMs}ms`);
			}
			throw e;
		} finally {
			clearTimeout(timer);
		}
	}

	listen(listener: Peer.Listener): Peer.Unlistener {
		this.listeners.add(listener);
		return () => {
			this.listeners.delete(listener);
		};
	}

	expectRaw<T>(
		message: Peer.Message<T>,
		matcher?: (raw: Uint8Array) => boolean,
	): Promise<Uint8Array> {
		return new Promise((resolve, reject) => {
			const unlisten = this.listen((msg) => {
				if (msg.command !== message.command) return;
				if (matcher && !matcher(msg.payload)) return;
				clearTimeout(tid);
				unlisten();
				resolve(msg.payload.slice());
			});
			const tid = setTimeout(() => {
				unlisten();
				reject(new Error(`Timeout waiting for ${message.command}`));
			}, 5_000);
		});
	}

	expect<T>(
		message: Peer.Message<T>,
		matcher?: (msg: T, raw: Uint8Array) => boolean,
	): Promise<T> {
		return new Promise((resolve, reject) => {
			const unlisten = this.listen((msg) => {
				if (msg.command !== message.command) return;
				const [data] = message.codec.decode(msg.payload.slice());
				if (matcher && !matcher(data, msg.payload)) return;
				clearTimeout(tid);
				unlisten();
				resolve(data);
			});
			const tid = setTimeout(() => {
				unlisten();
				reject(new Error(`Timeout waiting for ${message.command}`));
			}, 5_000);
		});
	}

	// -------- internals --------
	private async readLoop(conn: Deno.Conn): Promise<void> {
		const q = new ByteQueue(64 * 1024);
		const readBuf = new Uint8Array(READ_CHUNK);
		const magic = this.magic;

		try {
			while (this.#connected) {
				const n = await conn.read(readBuf);
				if (n === null) {
					// Connection closed by peer
					this.disconnect({ type: "connection_closed" });
					break;
				}
				if (n > 0) {
					q.append(readBuf, n);
				}

				// parse as much as possible
				parse: while (q.length >= HDR_LEN) {
					// align to magic
					if (!q.startsWith(magic)) {
						const idx = q.findMagic(magic);
						if (idx < 0) {
							// keep last 3 bytes to catch split magic across boundary
							const keep = Math.min(q.length, MAGIC_LEN - 1);
							q.consume(q.length - keep);
							break parse;
						}
						// drop garbage before magic
						q.consume(idx - (q.raw().r));
					}

					// we have magic at start
					if (q.length < HDR_LEN) break;

					const hdr = q.peek(HDR_LEN); // [magic(4), cmd(12), len(4), cs(4)]
					const payloadLen = u32le(hdr, 16);
					const frameLen = HDR_LEN + payloadLen;
					if (q.length < frameLen) break;

					const cmdStr = trimCmd(hdr.subarray(4, 16));
					const recvCS0 = hdr[20], recvCS1 = hdr[21], recvCS2 = hdr[22], recvCS3 = hdr[23];

					// payload view (zero-copy)
					const payload = q.peek(frameLen).subarray(HDR_LEN, frameLen);

					// checksum verify
					const calc = sha256(sha256(payload));
					if (
						calc[0] !== recvCS0 || calc[1] !== recvCS1 ||
						calc[2] !== recvCS2 || calc[3] !== recvCS3
					) {
						// skip corrupt frame
						q.consume(frameLen);
						continue parse;
					}

					// notify listeners synchronously with zero-copy view
					// NOTE: the view is ephemeral; process immediately.
					if (this.listeners.size) {
						for (const l of this.listeners) {
							try {
								l({ command: cmdStr, payload });
							} catch { /* ignore */ }
						}
					}

					// advance
					q.consume(frameLen);
				}
			}
		} catch (e) {
			// Read error - disconnect with reason
			this.disconnect({ type: "read_error", error: e });
		} finally {
			// Ensure disconnected (idempotent)
			if (this.#connected) {
				this.disconnect({ type: "connection_closed" });
			}
		}
	}
	log(...args: unknown[]): void {
		console.log(
			`\x1b[90m[\x1b[0m\x1b[36mPeer\x1b[0m\x1b[90m \x1b[0m\x1b[33m${this.remoteHost}\x1b[0m\x1b[90m:\x1b[0m\x1b[32m${this.remotePort}\x1b[0m\x1b[90m]\x1b[0m`,
			...args,
		);
	}

	logError(...args: unknown[]): void {
		console.error(
			`\x1b[90m[\x1b[0m\x1b[31mPeer\x1b[0m\x1b[90m \x1b[0m\x1b[33m${this.remoteHost}\x1b[0m\x1b[90m:\x1b[0m\x1b[32m${this.remotePort}\x1b[0m\x1b[90m]\x1b[0m`,
			...args,
		);
	}

	logWarn(...args: unknown[]): void {
		console.warn(
			`\x1b[90m[\x1b[0m\x1b[33mPeer\x1b[0m\x1b[90m \x1b[0m\x1b[33m${this.remoteHost}\x1b[0m\x1b[90m:\x1b[0m\x1b[32m${this.remotePort}\x1b[0m\x1b[90m]\x1b[0m`,
			...args,
		);
	}
}
