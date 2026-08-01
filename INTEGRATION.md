# Wiring the shared cursor/meta lock

The seqlock version word now lives in one process-wide `SharedArrayBuffer` (the
control block), so `Atomics` on it emit a real cross-worker fence. The payload
(cursor size, meta) stays in mmap exactly as before.

**This drop-in changes nothing until you do the 3 steps below.** Until an
isolate calls `markReady()`, seqlocks read/write mmap directly — i.e. today's
behavior. So you can land the files first and wire incrementally.

## 1. Root isolate (`main.ts`) — create it and forward it

```ts
import { controlSab, initRootControl, markReady } from "~/libs/storage/control.ts";

initRootControl();          // once, early — before spawning workers is fine
// ... spawn workers as you do now, but include the SAB in each init message:
p2pWorker.postMessage({ control: controlSab() }, [syncMessageChannel.port1]);
chainWorker.postMessage({ control: controlSab() }, [syncMessageChannel.port2]);
// (same for the server + gui workers' init messages)
markReady();                // main's own seqlocks now fence
```

Your boot work before this (`rollback()`, genesis seeding) runs in the boot
window — single-threaded, no writer — so it's safe on the direct path.

## 2. Every worker that touches a store — adopt it

At the top of the worker's init/message handler, BEFORE any store read/write:

```ts
import { markReady, setControl } from "~/libs/storage/control.ts";

// where you receive the init message (e.g. self.onmessage / the port message):
setControl(event.data.control);
markReady();
```

Workers that need this (they read or write a store): `p2p`, `chain`, `server`
(`app.worker`), `spender` (`spender.worker`), `consume` (`consume.worker`),
`gui`. The one that does NOT: `compress.worker` — it only streams files by path,
never touches a store.

## 3. Recursively-spawned workers — forward it down

Where the chain worker spawns children (`SpenderIndexer.ts`, the consume worker,
etc.), forward the same SAB it received — one field, unchanged:

```ts
import { controlSab } from "~/libs/storage/control.ts";
worker.postMessage({ control: controlSab(), /* ...rest... */ });
```

and each child does step 2.

## Notes

- Cursor and meta are unchanged on disk (the old `[u32][payload]` layout is kept;
  the leading u32 is now vestigial pad). No migration.
- If a worker forgets `setControl` but touches a store after `markReady()`, it
  throws loudly — it won't silently fall back to unfenced reads.
- Store ids are matched by 32-bit hash of the file path (see `SharedControl`).
  Distinct paths ⇒ effectively no collision risk; upgrade to storing id bytes if
  you ever want it provably impossible.
