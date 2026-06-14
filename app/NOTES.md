# notes

- rocksdb bindings we use dont support bloom filters yet, so just wait for an update for it. or you might change
  bindings as well.
- clean up blobstore to be more readable and better.
- clean up chain.ts instead turn it into a one nice class you can use.
- get rid of remaning Tx, TxInput, TxOutput etc classes in favor of pure Codecs, and pure helper functions.
- use namespaces in the backend, makes it more organized. its backend app code we dont have to worry about tree shaking.
  probably just export as object.
- turn it into a real app with real ui. so things start to fall into their place. instead of looking like a one big sync
  script. (makkeei titncoool sas s fuckck llsks) 
- maybe translating between Wire and Stored formated directly instead of doing the route
  `wire.decode() -> stored.encode()` might help a little maybe? but it would complicate the logic. and doesnt show up on
  the profile too much anyway.
- a better thing would be redesigning the sync logic, so it uses rocksdb in concurrent mass bursts with `Promise.all()`
  and stuff. so we dont wait for a single answer idling.
- while talking about the thing above, also maybe remove some helper function, they abstract too much, i cant remember
  what stores im using.
- another issue is now we dont wait for flush to end, flushes can take multiple ticks, which causes stage to grow so
  much, which causes next flush to take longer, which causes stage to grow more etc. this needs more than one solution,
  first if the memory usage starts to get near the max await the flush. another thing is, make the flush itself faster.
  and lastly incrise the v8 heap memory limit.
- while downloading make download ahead based on size not count, kinda slow during early blocks. probably guess block size based on average block size. start with max block size, go down from there. instead of making per block append txs, append all of the txs from multiple blocks at once. then only if the batch fails. retry again until the failed block's offset. the goal here is to have the least difference between small blocks, and big blocks during ibd. 
