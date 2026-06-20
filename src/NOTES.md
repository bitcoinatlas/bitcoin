# notes

- rocksdb bindings we use dont support bloom filters yet, so just wait for an update for it. or you might change bindings as well.
- clean up chain.ts instead turn it into a one nice class you can use.
- turn it into a real app with real ui. so things start to fall into their place. instead of looking like a one big sync script. (makkeei
  titncoool sas s fuckck llsks)
- process bottlenecked(mostly rocks and kv reads, indexes). rn. but i think im gonna stop trying to make it faster FOR NOW. im gonna focus
  on making it into an app now, fast enough for now later we can focus on making it even faster.

- ok the new storage system we made works right rn. need some more recactor to refine the like flow and interfaces better. but looks good
  rn. another thing is. with BlobStore we call fsync too much. so while refining the interface think about it ,probably make it happen at
  the end of the flush only. i thinking something like dirty flag for the chunks. so we know what to fsync.
- another thing about new storage system is, we didnt really make the IndexStore, llm made it. so sometime in the future also rewrite that
  as well.
- another thing to remember is, in BlobStore, we made open() files per get() call and close open close open close. we made this to make
  multi chunk reads and using seek with concurrency more stable, and simpler. but we can probably have reader pool, and queue if the every
  reader of a chunk is being used. or something like that probably. or maybe something else smarter? idk.

- ok so we made p2p and block download into its own worker, it works fine, but it now produces blocks faster we can consume them.
- so first thing is it seems we spend a lot of CPU time for the codecs, so we are gonna make some changes and updates to
  `@nomadshiba/codec`, and also custom codecs in this codebase
  - first of all, yes we accept `target` during encoding but we didnt have `offset` because we thought `.subarray()` would be enough. but it
    seems it still allocated a new `Uint8Array`, and V8 doesnt optimize it away. so we need to add offset argument to the codecs.
  - another thing is, yes we have `target` but we never use it in built it composite impls
  - also it seems even in this codebase we are not using target, we keep calling `encode()` alone. so handle that.
  - we can get a lot time back just by doing these. probably will make it fast enough.
- another target is basically rocksdb binding we use doesnt have blooms filters. this might make use gain some time as well

- also p2p/worker.ts is kinda messy, and some of chain/ChainStore.ts as well, rewrite it better some time
