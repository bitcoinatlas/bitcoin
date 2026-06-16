# notes

- rocksdb bindings we use dont support bloom filters yet, so just wait for an update for it. or you might change bindings as well.
- clean up blobstore to be more readable and better.
- clean up chain.ts instead turn it into a one nice class you can use.
- get rid of remaning Tx, TxInput, TxOutput etc classes in favor of pure Codecs, and pure helper functions.
- use namespaces in the backend, makes it more organized. its backend app code we dont have to worry about tree shaking. probably just
  export as object.
- turn it into a real app with real ui. so things start to fall into their place. instead of looking like a one big sync script. (makkeei
  titncoool sas s fuckck llsks)
- maybe translating between Wire and Stored formated directly instead of doing the route `wire.decode() -> stored.encode()` might help a
  little maybe? but it would complicate the logic. and doesnt show up on the profile too much anyway.
- a better thing would be redesigning the sync logic, so it uses rocksdb in concurrent mass bursts with `Promise.all()` and stuff. so we
  dont wait for a single answer idling.
- while talking about the thing above, also maybe remove some helper function, they abstract too much, i cant remember what stores im using.
- another issue is now we dont wait for flush to end, flushes can take multiple ticks, which causes stage to grow so much, which causes next
  flush to take longer, which causes stage to grow more etc. this needs more than one solution, first if the memory usage starts to get near
  the max await the flush. another thing is, make the flush itself faster. and lastly incrise the v8 heap memory limit.
- while downloading make download ahead based on size not count, kinda slow during early blocks. probably guess block size based on average
  block size. start with max block size, go down from there. instead of making per block append txs, append all of the txs from multiple
  blocks at once. then only if the batch fails. retry again until the failed block's offset. the goal here is to have the least difference
  between small blocks, and big blocks during ibd.
- process bottlenecked(mostly rocks and kv reads, indexes). rn. but i think im gonna stop trying to make it faster FOR NOW. im gonna focus
  on making it into an app now, fast enough for now later we can focus on making it even faster.
- an alternative to current WAL interface. purely all or none orianted. no wal file. just a file indicating the size of the current file. so
  basically we append to file but dont update its size, if any flush for any store files, we just truncate back to the size during recovery.
  so we still have on disk staged data. thats not changed. but we dont write to a WAL first. instead we write to the actual file and on fail
  we just truncate back. for the kv with rocks we can use rocks transactions i think. and force rocks to flush(). interface is like we
  flush(), then we can rollback() the last flush. because we know the previous size. for blob, array, and index store its easy, we might
  have to think more about kv. i mean kv can remember its old state, but thats extra lookup which is already slow enough. rn we are
  basically doubling every write which is not good. anyway so flush(), then rollback() if we wanna rollback() the disk. when flush() starts
  we freeze the stage move its reference. when flush is done we get rid of the frozen stage, and also update the realized in memory size.
  and until we start another flush(), we can always rollback(). so idk how thats gonna work with rocks. if rocks has a feature that gives as
  the old value during set. we can utilize that probably? have an anti-wal file? i mean whole point is shit failed. disk didnt change right?
  maybe we can make Atomic support both kinds of stores and use them together or something? anyway look into it while writing it.
