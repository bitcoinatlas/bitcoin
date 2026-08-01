import { SharedControl } from "~/libs/storage/SharedControl.ts";

/**
 * The process control block, per isolate. The root isolate calls
 * {@linkcode initRootControl}; every worker that touches a store calls
 * {@linkcode setControl} with the forwarded SAB, then {@linkcode markReady}.
 *
 * Boot window: before {@linkcode markReady}, an isolate runs single-threaded
 * (recovery, genesis, rehash) with no concurrent writer, so seqlocks read/write
 * mmap directly — no fence needed. After {@linkcode markReady}, a seqlock with
 * no control bound would mean unfenced cross-worker reads, so that throws
 * loudly instead of silently tearing on ARM.
 */
let control: SharedControl | null = null;
let ready = false;

/** Root isolate: create the one control block. Returns it (use `.sab` to forward). */
export function initRootControl(bytes?: number): SharedControl {
	control = SharedControl.create(bytes);
	return control;
}

/** Worker: adopt the forwarded control block. Call before doing any store work. */
export function setControl(sab: SharedArrayBuffer): void {
	control = new SharedControl(sab);
}

/** Close the boot window: from here, touching a store without a control block is an error. */
export function markReady(): void {
	ready = true;
}

/** The SAB to forward to a child worker. Throws if this isolate has no control block. */
export function controlSab(): SharedArrayBuffer {
	if (!control) throw new Error("control block not initialised in this isolate");
	return control.sab;
}

/**
 * For {@linkcode Seqlock}: the bound control, or `null` during the boot window.
 * Throws after {@linkcode markReady} if still unset — that would be an isolate
 * doing unfenced cross-worker reads.
 */
export function getStorageControl(): SharedControl | null {
	if (control) return control;
	if (ready) {
		throw new Error(
			"storage control block missing after markReady() — this isolate must setControl(sab) before touching stores concurrently",
		);
	}
	return null;
}
