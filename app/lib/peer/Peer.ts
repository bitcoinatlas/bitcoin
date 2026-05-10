import { type Codec } from "@nomadshiba/codec";
import { sha256 } from "@noble/hashes/sha2";

const MAGIC_LEN = 4;
const CMD_LEN = 12;
const HDR_LEN = 24; // magic(4) + cmd(12) + len(4) + checksum(4)
const READ_CHUNK = 32 * 1024;

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
	let end = buf.length;
	while (end > 0 && buf[end - 1] === 0) end--;
	return ASCII.decode(buf.subarray(0, end));
}

export type PeerMessageEvent = {
	command: string;
	payload: Uint8Array;
};

export type DisconnectReason =
	| { type: "manual" }
	| { type: "read_error"; error: unknown }
	| { type: "connection_closed" }
	| { type: "write_timeout" };

export type PeerMessage<T> = {
	command: string;
	codec: Codec<T>;
};

function makePeerMessage(command: string, payload: Uint8Array): PeerMessageEvent {
	const msg: PeerMessageEvent = {
		command,
		payload,
	};
	return msg;
}

export class Peer {
	readonly host: string;
	readonly port: number;
	readonly #magic: Uint8Array;

	#connected = false;
	#conn: Deno.Conn | null = null;
	#listeners = new Set<(msg: PeerMessageEvent) => void>();
	#disconnectCallbacks = new Set<(reason: DisconnectReason) => void>();

	get connected(): boolean {
		return this.#connected;
	}

	constructor(host: string, port: number, magic: Uint8Array) {
		if (magic.length !== MAGIC_LEN) throw new Error("magic must be 4 bytes");
		this.host = host;
		this.port = port;
		this.#magic = magic;
	}

	async connect(timeoutMs = 5_000): Promise<void> {
		if (this.#connected) return;
		const abort = new AbortController();
		const timer = setTimeout(() => abort.abort(), timeoutMs);
		try {
			this.#conn = await Deno.connect({
				hostname: this.host,
				port: this.port,
				transport: "tcp",
				signal: abort.signal,
			});
		} finally {
			clearTimeout(timer);
		}
		this.#connected = true;
		void this.#readLoop(this.#conn);
	}

	disconnect(reason: DisconnectReason = { type: "manual" }): void {
		if (!this.#connected) return;
		this.#connected = false;
		try {
			this.#conn?.close();
		} catch { /* noop */ }
		this.#conn = null;
		for (const cb of this.#disconnectCallbacks) {
			try {
				cb(reason);
			} catch { /* noop */ }
		}
	}

	onDisconnect(cb: (reason: DisconnectReason) => void): () => void {
		this.#disconnectCallbacks.add(cb);
		return () => this.#disconnectCallbacks.delete(cb);
	}

	onMessage(listener: (msg: PeerMessageEvent) => void): () => void {
		this.#listeners.add(listener);
		return () => this.#listeners.delete(listener);
	}

	async send<T>(def: PeerMessage<T>, data: T, timeoutMs = 10_000): Promise<void> {
		const conn = this.#conn;
		if (!this.#connected || !conn) throw new Error("not connected");

		const { command: cmd, codec } = def;
		if (cmd.length < 1 || cmd.length > CMD_LEN) throw new Error("invalid command length");
		for (let i = 0; i < cmd.length; i++) {
			const c = cmd.charCodeAt(i);
			if (c < 0x20 || c > 0x7e) throw new Error("command must be printable ASCII");
		}

		const payload = codec.encode(data);
		const frame = new Uint8Array(HDR_LEN + payload.length);

		frame.set(this.#magic, 0);
		const cmdBytes = ASCII_ENC.encode(cmd);
		frame.set(cmdBytes, 4);
		frame.fill(0, 4 + cmdBytes.length, 16);
		putU32le(frame, 16, payload.length);
		const cs = sha256(sha256(payload));
		frame[20] = cs[0]!;
		frame[21] = cs[1]!;
		frame[22] = cs[2]!;
		frame[23] = cs[3]!;
		frame.set(payload, HDR_LEN);

		const abort = new AbortController();
		const timer = setTimeout(() => abort.abort(), timeoutMs);
		try {
			await conn.write(frame);
		} catch (e) {
			if (abort.signal.aborted) {
				this.disconnect({ type: "write_timeout" });
				throw new Error(`write timeout after ${timeoutMs}ms`);
			}
			throw e;
		} finally {
			clearTimeout(timer);
		}
	}

	expect<T>(def: PeerMessage<T>, timeoutMs = 5_000): Promise<T> {
		return new Promise((resolve, reject) => {
			const tid = setTimeout(() => {
				unlisten();
				reject(new Error(`timeout waiting for ${def.command}`));
			}, timeoutMs);

			const unlisten = this.onMessage((msg) => {
				if (msg.command !== def.command) return;
				clearTimeout(tid);
				unlisten();
				const [data] = def.codec.decode(msg.payload);
				resolve(data);
			});
		});
	}

	async #readLoop(conn: Deno.Conn): Promise<void> {
		let buf = new Uint8Array(64 * 1024);
		let len = 0;
		const tmp = new Uint8Array(READ_CHUNK);
		const magic = this.#magic;

		const grow = (need: number) => {
			let cap = buf.length;
			while (cap - len < need) cap *= 2;
			if (cap !== buf.length) {
				const nb = new Uint8Array(cap);
				nb.set(buf.subarray(0, len));
				buf = nb;
			}
		};

		try {
			while (this.#connected) {
				const n = await conn.read(tmp);
				if (n === null) {
					this.disconnect({ type: "connection_closed" });
					return;
				}
				if (n === 0) continue;

				grow(n);
				buf.set(tmp.subarray(0, n), len);
				len += n;

				let off = 0;
				parse: while (len - off >= HDR_LEN) {
					if (
						buf[off] !== magic[0] || buf[off + 1] !== magic[1] ||
						buf[off + 2] !== magic[2] || buf[off + 3] !== magic[3]
					) {
						let found = -1;
						const limit = len - MAGIC_LEN + 1;
						for (let i = off + 1; i < limit; i++) {
							if (
								buf[i] === magic[0] && buf[i + 1] === magic[1] && buf[i + 2] === magic[2] &&
								buf[i + 3] === magic[3]
							) {
								found = i;
								break;
							}
						}
						if (found < 0) {
							off = Math.max(off, len - (MAGIC_LEN - 1));
							break parse;
						}
						off = found;
					}

					if (len - off < HDR_LEN) break;

					const payloadLen = u32le(buf, off + 16);
					const frameLen = HDR_LEN + payloadLen;
					if (len - off < frameLen) break;

					const command = trimCmd(buf.subarray(off + 4, off + 16));
					const recvCs = buf.subarray(off + 20, off + 24);
					const payload = buf.subarray(off + HDR_LEN, off + frameLen);

					const calc = sha256(sha256(payload));
					if (
						calc[0] === recvCs[0] && calc[1] === recvCs[1] && calc[2] === recvCs[2] && calc[3] === recvCs[3]
					) {
						const msg = makePeerMessage(command, payload.slice());
						for (const l of this.#listeners) {
							try {
								l(msg);
							} catch { /* noop */ }
						}
					}

					off += frameLen;
				}

				if (off > 0) {
					buf.copyWithin(0, off, len);
					len -= off;
				}
			}
		} catch (e) {
			this.disconnect({ type: "read_error", error: e });
		} finally {
			if (this.#connected) this.disconnect({ type: "connection_closed" });
		}
	}
}
