# BitcoinAtlas

Experimental Bitcoin node and client. Still a work in progress, **not** ready for real use yet.

For learning, hacking, or just messing around (for now). Still way too early.

Fully written in verifiable Deno + TypeScript, so it's friendly to more devs and easy to inspect.

The main goal is to **optimize storage** — reduce a fully-indexed node to less than half the size of a typical
implementation, without sacrificing any features.

We prioritize UX and simplicity. The end user is a normal everyday Bitcoin user, not a protocol nerd.

We are not trying to change Bitcoin. We are simply giving you a way to use Bitcoin in the best way possible, with less
storage and less friction.

## Notes

### Technical Notes and Terminology

- The whole codebase uses **wire format internally**, so there is no `.reverse()` or `.toReversed()` anywhere in the
  core logic, except while making things human-readable on ui or logs.
- I call the original implementation of Bitcoin and anything legacy **"satoshi"**. For example:
  - Satoshi Client
  - Satoshi RPC
  - Satoshi P2P
  - `computeSatoshiMerkleRoot`
  - "satoshi address type"
- Internally, I use **4 MB as max block weight**:
  - Witness is weighted 1×
  - Non-witness data is weighted 4×
- `computeSatoshiMerkleRoot` returns **empty bytes (void)** on "mutated = true" instead of a `[hash, mutated]` pair.
  There is no `mutated` boolean.
- I call the mempool **txpool**. "mempool" is a misnomer that stuck; txpool is more accurate.
- There is no such thing as a pruned node. Pruned nodes are not useful in a meaningful way. BitcoinAtlas is always a
  full node with full history.

### Storage Philosophy

A typical fully-featured node setup today requires roughly 1 TB across multiple apps:

- ~855 GB — Satoshi client (Core/Knots) with full indexes
- ~108 GB — Electrum server
- ~40 GB — mempool.space / block explorer

These tools store redundant data because they can't trust each other and must each be self-contained. BitcoinAtlas is a
monolithic implementation that can make assumptions they can't:

- Prevouts never store txid twice — they use a pointer back to the tx record.
- Pubkeys are stored once and referenced by pointer everywhere they repeat.
- Spent state is a single bit flag inline in the output record — no separate UTXO set.
- No raw block storage — only structured, compact tx data.
- Smaller integer types throughout, because domain bounds are known (e.g. 21M BTC fits in 51 bits, block height fits in
  u28 for centuries, vout index fits in u16 for all real-world transactions).
- Script types (P2PKH, P2WPKH, P2SH, P2WSH, P2TR) stored as type tag + hash only — no redundant script encoding.

Target: **600 GB or less** with the same feature set, using encoding optimizations alone. With chunk compression on cold
data (e.g. LZ4), potentially **200–400 GB**.

## Short-Term Goal

- Install Termux on your phone.
- `pkg install deno`
- `deno run -A bitcoinatlas.ts`
- Have a full node with full history, **running on your phone** and actually fitting in its storage.

## Long-Term Goals

- Be a full node with **aggressive storage optimization**.
- Support **Satoshi RPC** endpoints.
- Support **Electrum** endpoints.
- Provide a **web app interface** on `localhost`:
  - Ship a webview as a desktop / mobile app GUI.
  - Include a mempool.space-like explorer built-in, with a scrollable block size chart where visual weight maps to
    actual block weight.
  - Support isolated WASM plugins for txpool filters and other logic (for example, DATUM).
  - Plugin "store" over Nostr.
  - Built-in plugins:
    - DATUM
    - Filtering plugins
    - Delayed propagation of blocks that don't fit your filters, based on the weight of "bad" transactions (e.g. delay
      5–10 minutes, maybe up to 1 hour max depending on weight).
- Eventually, when everything else is done, introduce a **new communication protocol over HTTP and WebSockets**, with
  optional PoW requirements for read requests.
- Unlike Satoshi clients, this should work out of the box **without** downloading the entire chain first:
  - It behaves like a light client at the beginning and downloads missing block data on demand.
  - For example: as you scroll the explorer block list, it lazily fetches the block data you're looking at — in ranges,
    not single blocks, to amortize request overhead and pre-warm adjacent blocks.
  - On-demand fetched blocks are stored in a separate cache; the background sync job checks the cache first before
    requesting from peers, and drains it into main storage as sync catches up.
  - It still validates block data and handles chain reorgs.
  - Background workers download missing historical block data from genesis to tip, with a bandwidth cap you set.
  - At the end of the day, you still have the whole chain downloaded and validated, but you don't need to wait for full
    sync to start using the client.
  - Blocks are trustlessly viewable as soon as headers are synced (hash matches header chain), even before full
    validation. Confirmed payment verification requires validation to have reached that height.
- Make it work well on **mobile devices**, which will require even better storage optimizations.
- Let you do **everything Bitcoin-related** from this client, without needing to set up complex third-party software and
  manually glue everything together. Plugins + built-in features handle it.
- So your **grandma** can run a useful node.

## LICENSE

[GPL v2](LICENSE)
