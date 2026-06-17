# notes

- rocksdb bindings we use dont support bloom filters yet, so just wait for an update for it. or you might change bindings as well.
- clean up chain.ts instead turn it into a one nice class you can use.
- use namespaces in the backend, makes it more organized. its backend app code we dont have to worry about tree shaking. probably just
  export as object.
- turn it into a real app with real ui. so things start to fall into their place. instead of looking like a one big sync script. (makkeei
  titncoool sas s fuckck llsks)
- maybe translating between Wire and Stored formated directly instead of doing the route `wire.decode() -> stored.encode()` might help a
  little maybe? but it would complicate the logic. and doesnt show up on the profile too much anyway.
- a better thing would be redesigning the sync logic, so it uses rocksdb in concurrent mass bursts with `Promise.all()` and stuff. so we
  dont wait for a single answer idling.
- while talking about the thing above, also maybe remove some helper function, they abstract too much, i cant remember what stores im using.
- while downloading make download ahead based on size not count, kinda slow during early blocks. probably guess block size based on average
  block size. start with max block size, go down from there. instead of making per block append txs, append all of the txs from multiple
  blocks at once. then only if the batch fails. retry again until the failed block's offset. the goal here is to have the least difference
  between small blocks, and big blocks during ibd.
- process bottlenecked(mostly rocks and kv reads, indexes). rn. but i think im gonna stop trying to make it faster FOR NOW. im gonna focus
  on making it into an app now, fast enough for now later we can focus on making it even faster.

- ok the new storage system we made works right rn. need some more recactor to refine the like flow and interfaces better. but looks good
  rn. another thing is. with BlobStore we call fsync too much. so while refining the interface think about it ,probably make it happen at
  the end of the flush only. i thinking something like dirty flag for the chunks. so we know what to fsync.
- another think about new storage system is, we didnt really make the IndexStore, llm made it. so sometime in the future also rewrite that
  as well.
- another thing to remember is, in BlobStore, we made open() files per get() call and close open close open close. we made this to make
  multi chunk reads and using seek with concurrency more stable, and simpler. but we can probably have reader pool, and queue if the every
  reader of a chunk is being used. or something like that probably. or maybe something else smarter? idk.
