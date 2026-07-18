# BitcoinAtlas

Experimental Bitcoin node and client. Still a work in progress, **not** ready for real use yet.

For learning, hacking, or just messing around (for now). Still way too early.

Fully written in verifiable Deno + TypeScript, so it's friendly to more devs and easy to inspect.

The main goal is to **optimize storage** — reduce a fully-indexed node to less than half the size of a typical implementation, without
sacrificing any features.

We prioritize UX and simplicity. The end user is a normal everyday Bitcoin user, not a protocol nerd.

We are not trying to change Bitcoin. We are simply giving you a way to use Bitcoin in the best way possible, with less storage and less
friction.

## Why? What?

### Why Deno + TypeScript?

Main thing we wanna work on here is storage optimization, and language selection is not a big part of that. And I believe we can make it
work "fast enough" with pure Deno + TypeScript.

We wanna keep the barrier to entry as low as possible, and make it easy for more people to contribute. Deno + TypeScript is a great choice
for that, since it's more widely known and easier to work with than Rust or C++.

### If TypeScript, Why Deno, and not Bun or NodeJS

- Permission model (`--allow-read`, `--allow-net`, etc.)
- Web-standard APIs
- Workers
- TypeScript support
- Single executable distribution
- Reasonably clean standard library
- No `node_modules` explosion

Permissions model for plugins, particularly attractive:

```ts
new Worker(pluginUrl, {
	type: "module",
	deno: {
		permissions: {
			read: false,
			write: false,
			net: false,
		},
	},
});
```

These plugins can also include WASM as well without telling us btw.

Deno is good because it rejects the mistakes of NodeJS, not support them like Bun does.

### Why another node implementation?

My goal is not to make just another node implementation. My goal is to make a bitcoin app where everything works out of the box as intended
bitcoin experience. Not just a node, but also a web interface, and a desktop/mobile app. A complete package where you can do everything
bitcoin-related without needing to set up complex third-party software and manually glue everything together.

Storage optimization is a big part of that, because it makes it more accessible to people with less storage space, and also just makes it
more efficient and elegant.

For example I run my bitcoin node on my old laptop with 1TB HDD and 1TB SSD, and i have to use symlinks and other hacks to split the data
across the two drives, and even then it's still a tight fit. With better storage optimization, I could fit the whole thing on my SSD and
have a much better experience.

There is a reason why there is no bitcoin node running on my personel main laptop, and it's because of the storage requirements. With
BitcoinAtlas, I want to change that and make it possible for anyone to run a full node on their personal laptop, or even their phone maybe
in the future. My old laptop is soon gonna run out of storage space and with this project I can keep running a full node on it without
needing to upgrade the storage.

### What is my dream end product?

A monolithic all-in-one Bitcoin App, that has everything you need. That uses half the storage and fully indexed still.

And maybe with nostr integration and plugins and stuff, who knows.

I also wanna build another communication protocol on top of it using WebSockets instead of raw TPC. Make it support multiple p2p protocols
at the same time, and let users choose which ones they wanna use at the same time. Maybe even support some kind of PoW requirement for
requests for reads, to prevent spam and DoS attacks.

And maybe later add p2p nodes over bluetooth. Or even radio maybe.

And more crazy stuff that I can't even think of right now. But most important point is the storage optimization, and making it accessible
and easy to use for everyone.

If my bitcoin node takes more than a few AAA games, then it's not good enough. I want it to be something that anyone can run on their
personal devices without needing to worry about storage space.

## Notes

### Technical Notes and Terminology

- The whole codebase uses **wire format internally**, so there is no `.reverse()` or `.toReversed()` anywhere in the core logic, except
  while making things human-readable on ui or logs.
- I call the original implementation of Bitcoin and anything legacy **"satoshi"**. For example:
  - Satoshi Client
  - Satoshi RPC
  - Satoshi P2P
  - `satoshiMerkleRoot`
  - "satoshi address type"
- Internally, I use **4 MB as max block weight**:
  - Witness is weighted 1×
  - Non-witness data is weighted 4×
- `satoshiMerkleRoot` returns **empty bytes (void)** on "mutated = true" instead of a `[hash, mutated]` pair. There is no `mutated` boolean.
- I call the mempool **txpool**. "mempool" is a misnomer that stuck; txpool is more accurate. (or maybe txcache and blocktemplate, can have
  both as seperate)
- There is no such thing as a pruned node. Pruned nodes are not useful in a meaningful way. BitcoinAtlas is always a full node with full
  history.

## Long-Term Goals

- Be a full node with **aggressive storage optimization**.
- Support **Satoshi RPC** endpoints.
- Support **Electrum** endpoints.
- Provide a **web app interface** on `localhost`:
  - Ship a webview as a desktop / mobile app GUI.
  - Include a mempool.space-like explorer built-in, with a scrollable block size chart where visual weight maps to actual block weight.
  - Support isolated WASM plugins for txpool filters and other logic (for example, DATUM).
  - Plugin "store" over Nostr.
  - Built-in plugins:
    - DATUM
    - Filtering plugins
    - Delayed propagation of blocks that don't fit your filters, based on the weight of "bad" transactions (e.g. delay 5–10 minutes, maybe
      up to 1 hour max depending on weight).
- Eventually, when everything else is done, introduce a **new communication protocol over WebSockets**, with optional PoW requirements for
  read requests. So you can even impl a light node on the browser. Or as a browser extension. Also when Atlas nodes discover each-other and
  talking over this network, for ibd they should share compressed chunks files directly. Instead of each block one by one. This means less
  bandwith requirement. And less of a need to compress while feeding new nodes doing IBD. and also faster sync time.
- Unlike Satoshi clients, this should work out of the box **without** downloading the entire chain first:
  - It behaves like a light client at the beginning and downloads missing block data on demand.
  - For example: as you scroll the explorer block list, it lazily fetches the block data you're looking at — in ranges, not single blocks,
    to amortize request overhead and pre-warm adjacent blocks.
  - On-demand fetched blocks are stored in a separate cache; the background sync job checks the cache first before requesting from peers,
    and drains it into main storage as sync catches up.
  - It still validates block data and handles chain reorgs.
  - Background workers download missing historical block data from genesis to tip, with a bandwidth cap you set.
  - At the end of the day, you still have the whole chain downloaded and validated, but you don't need to wait for full sync to start using
    the client.
  - Blocks are trustlessly viewable as soon as headers are synced (hash matches header chain), even before full validation. Confirmed
    payment verification requires validation to have reached that height.
- Make it work well on **mobile devices**, which will require even better storage optimizations.
- Let you do **everything Bitcoin-related** from this client, without needing to set up complex third-party software and manually glue
  everything together. Plugins + built-in features handle it.
- So your **grandma** can run a useful node.

## LICENSE

[GPL v2](LICENSE)
