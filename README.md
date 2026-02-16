# BitcoinAtlas

Experimental Bitcoin node and client. Still a work in progress, **not** ready for real use yet.

For learning, hacking, or just messing around (for now). Still way too early.

Fully written in verifiable Deno + TypeScript, so it’s friendly to more devs and easy to inspect.

The main goal is to **optimize storage** (reduce indexed full-node size to less than half of a typical implementation).

We prioritize UX and simplicity. The end user is a normal everyday Bitcoin user, not a protocol nerd.

We are not trying to change Bitcoin. We are simply giving you a way to use Bitcoin in the best way possible, with less
storage and less friction.

---

## ~~Update: I changed my mind~~

~~I’ve been working on this for a while, and the more I build, the more I feel this project would be even better (and
more understandable) with ECS and game loops.~~

~~**Current plan:**~~

~~1. First, make the storage optimization work with the current Deno/TypeScript codebase and actually get a full node
that takes significantly less disk space.~~ ~~2. After that succeeds, I’ll move the implementation to modern C# using an
ECS library (https://github.com/Felid-Force-Studios/StaticEcs).~~

~~Deno/TS is for fast iteration and proving the storage model.\
C# + ECS will be for long-term performance, parallelism, and game-loop style logic.~~

---

## Notes

- The whole codebase uses **wire format internally**, so there is no `.reverse()` or `.toReversed()` anywhere in the
  core logic, except when making things like console logs human-readable.
- I call the original implementation of Bitcoin and anything legacy **“satoshi”**. For example:
  - Satoshi client
  - Satoshi RPC
  - Satoshi TPC or Satoshi P2P
  - `computeSatoshiMerkleRoot`
  - “satoshi address type”
- Internally, I use **4 MB as max block weight**:
  - Witness is weighted 1×
  - Non-witness data is weighted 4×
- `computeSatoshiMerkleRoot` returns **empty bytes (void)** instead of a `[hash, mutated]` pair. There is no `mutated`
  boolean.

---

## Short-Term Goal

- Install Termux on your phone.
- `pkg install deno`
- `deno run -A bitcoinatlas.ts`
- Have a full node with full history, **running on your phone** and actually fitting in its storage.

---

## Long-Term Goals

- Be a full node with **aggressive storage optimization**.
- Support **Satoshi RPC** endpoints.
- Support **Electrum** endpoints.
- Provide a **web app interface** on `localhost`:
  - Ship a webview as a desktop / mobile app GUI.
  - Include a mempool.space-like explorer built-in.
  - Support isolated WASM plugins for mempool filters and other logic (for example, DATUM).
  - Plugin “store” over Nostr.
  - Built-in plugins:
    - DATUM
    - Filtering plugins
    - Delayed propagation of blocks that don’t fit your filters, based on the weight of “bad” transactions (e.g. delay
      5–10 minutes, maybe up to 1 hour max depending on weight).
- Eventually, when everything else is done, introduce a **new communication protocol over HTTP and WebSockets**, with
  optional PoW requirements even for read requests.
- Unlike Satoshi clients, this should work out of the box **without** downloading the entire chain first:
  - It can behave like a light client at the beginning and download missing block data on demand.
  - For example: as you scroll the explorer block list, it lazily fetches the block data you’re looking at.
  - It still validates block data and handles chain reorgs.
  - Background workers will download missing historical block data from genesis to tip, with a bandwidth cap you set.
  - At the end of the day, you still have the whole chain downloaded and validated, but you don’t need to wait for full
    sync to start using the client.
- Make it work well on **mobile devices**, which will require even better storage optimizations.
- Let you do **everything Bitcoin-related** from this client, without needing to set up complex third-party software and
  manually glue everything together. Plugins + built-in features handle it.
- So your **grandma** can run a useful node.
