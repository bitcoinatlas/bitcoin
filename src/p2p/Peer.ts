import { sha256 } from "@noble/hashes/sha2";
import { Codec } from "@nomadshiba/codec";
import { KiB, SECOND } from "~/constants.ts";

const MAGIC_LEN = 4;
const CMD_LEN = 12;
const HDR_LEN = 24; // magic(4) + cmd(12) + len(4) + checksum(4)
const READ_CHUNK = 32 * KiB;
const DEFAULT_TIMEOUT_MS = 30 * SECOND;

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

export type PeerMessage<Sent extends Codec> = {
	command: string;
	codec: Sent;
};

function makePeerMessage(command: string, payload: Uint8Array): PeerMessageEvent {
	const msg: PeerMessageEvent = {
		command,
		payload,
	};
	return msg;
}

export class Peer {
	public readonly host: string;
	public readonly port: number;
	public readonly magic: Uint8Array;

	private isConnected = false;
	private connection: Deno.Conn | null = null;
	private listeners = new Set<(msg: PeerMessageEvent) => void>();
	private disconnectCallbacks = new Set<(reason: DisconnectReason) => void>();

	/** Remote peer's version payload, populated after handshake. */
	remoteVersion: number = 0;
	/** Remote peer's advertised services bitmask, populated after handshake. */
	remoteServices: bigint = 0n;

	get connected(): boolean {
		return this.isConnected;
	}

	constructor(host: string, port: number, magic: Uint8Array) {
		if (magic.length !== MAGIC_LEN) throw new Error("magic must be 4 bytes");
		this.host = host;
		this.port = port;
		this.magic = magic;
	}

	async connect(timeoutMs = DEFAULT_TIMEOUT_MS): Promise<void> {
		if (this.isConnected) return;
		const abort = new AbortController();
		const timer = setTimeout(() => abort.abort(), timeoutMs);
		try {
			this.connection = await Deno.connect({
				hostname: this.host,
				port: this.port,
				transport: "tcp",
				signal: abort.signal,
			});
		} finally {
			clearTimeout(timer);
		}
		this.isConnected = true;
		void this.readLoop(this.connection);
	}

	disconnect(reason: DisconnectReason = { type: "manual" }): void {
		if (!this.isConnected) return;
		this.isConnected = false;
		try {
			this.connection?.close();
		} catch { /* noop */ }
		this.connection = null;
		for (const cb of this.disconnectCallbacks) {
			try {
				cb(reason);
			} catch { /* noop */ }
		}
	}

	onDisconnect(cb: (reason: DisconnectReason) => void): () => void {
		this.disconnectCallbacks.add(cb);
		return () => this.disconnectCallbacks.delete(cb);
	}

	onMessage(listener: (msg: PeerMessageEvent) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	async send<T extends Codec>(type: PeerMessage<T>, data: Codec.InferInput<T>, timeoutMs = DEFAULT_TIMEOUT_MS): Promise<void> {
		const connection = this.connection;
		if (!this.isConnected || !connection) throw new Error("not connected");

		const { command, codec } = type;
		if (command.length < 1 || command.length > CMD_LEN) throw new Error("invalid command length");
		for (let i = 0; i < command.length; i++) {
			const c = command.charCodeAt(i);
			if (c < 0x20 || c > 0x7e) throw new Error("command must be printable ASCII");
		}

		const payload = codec.encode(data);
		const frame = new Uint8Array(HDR_LEN + payload.length);

		frame.set(this.magic, 0);
		const commandBytes = ASCII_ENC.encode(command);
		frame.set(commandBytes, 4);
		frame.fill(0, 4 + commandBytes.length, 16);
		putU32le(frame, 16, payload.length);
		const cs = sha256(sha256(payload));
		frame[20] = cs[0]!;
		frame[21] = cs[1]!;
		frame[22] = cs[2]!;
		frame[23] = cs[3]!;
		frame.set(payload, HDR_LEN);

		// Deno.Conn.write can't be aborted by a signal, so enforce the timeout by
		// closing the connection — that rejects the pending write. Also loop, since
		// write may report a partial count and the protocol needs the whole frame.
		let timedOut = false;
		const timer = setTimeout(() => {
			timedOut = true;
			this.disconnect({ type: "write_timeout" });
		}, timeoutMs);
		try {
			let written = 0;
			while (written < frame.length) {
				const n = await connection.write(frame.subarray(written));
				if (n <= 0) throw new Error("connection closed during write");
				written += n;
			}
		} catch (e) {
			if (timedOut) throw new Error(`write timeout after ${timeoutMs}ms`);
			throw e;
		} finally {
			clearTimeout(timer);
		}
	}

	expect<T extends Codec>(
		type: PeerMessage<T>,
		filter?: (data: Codec.InferOutput<T>, bytes: Uint8Array) => boolean,
		timeoutMs = DEFAULT_TIMEOUT_MS,
	): Promise<[Codec.InferOutput<T>, Uint8Array]> {
		return new Promise((resolve, reject) => {
			const tid = setTimeout(() => {
				unlisten();
				reject(new Error(`timeout waiting for ${type.command}`));
			}, timeoutMs);

			const unlisten = this.onMessage((message) => {
				if (message.command !== type.command) return;
				const [data] = type.codec.decode(message.payload);
				if (filter && !filter(data, message.payload)) return; // not ours — keep waiting
				clearTimeout(tid);
				unlisten();
				resolve([data, message.payload]);
			});
		});
	}

	async sendAndExpect<Outgoing extends Codec, Incoming extends Codec>(params: {
		send: {
			type: PeerMessage<Outgoing>;
			data: Codec.InferInput<Outgoing>;
		};
		receive: {
			type: PeerMessage<Incoming>;
			filter(data: Codec.InferOutput<Incoming>, bytes: Uint8Array): boolean;
		};
		timeoutMs?: number;
	}): Promise<[Codec.InferOutput<Incoming>, Uint8Array]> {
		const timeout = params.timeoutMs ?? DEFAULT_TIMEOUT_MS;
		const promise = this.expect(params.receive.type, params.receive.filter, timeout);
		await this.send(params.send.type, params.send.data, timeout);
		return promise;
	}

	private async readLoop(conn: Deno.Conn): Promise<void> {
		let buf = new Uint8Array(64 * KiB);
		let len = 0;
		const tmp = new Uint8Array(READ_CHUNK);
		const magic = this.magic;

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
			while (this.isConnected) {
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
						for (const l of this.listeners) {
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
			if (this.isConnected) this.disconnect({ type: "connection_closed" });
		}
	}
}
