# immediate goal (current)

- [ ] sync to the tip, see if it can reach to the tip. then make the ui, check the timechain see if anything is off.

- [ ] then focus on more speed + storage optimizations.

- [ ] more stable peer communication interface.

- [ ] clean up the codebase.

- [ ] impl on-demand block downloads.

- [ ] make it look cool. configs and stuff.

- [ ] then do relay p2p stuff

# notes

- [ ] turn it into a real app with real ui. so things start to fall into their place. instead of looking like a one big sync script.
      (makkeei titncoool sas s fuckck llsks)

- [ ] ok the new storage system we made works right rn. need some more recactor to refine the like flow and interfaces better. but looks
      good rn. another thing is. with BlobStore we call fsync too much. so while refining the interface think about it ,probably make it
      happen at the end of the flush only. i thinking something like dirty flag for the chunks. so we know what to fsync.
- [ ] another thing about new storage system is, we didnt really make the IndexStore, llm made it. so sometime in the future also rewrite
      that as well.
- [ ] another thing to remember is, in BlobStore, we made open() files per get() call and close open close open close. we made this to make
      multi chunk reads and using seek with concurrency more stable, and simpler. but we can probably have reader pool, and queue if the
      every reader of a chunk is being used. or something like that probably. or maybe something else smarter? idk.

- [ ] ok so we made p2p and block download into its own worker, it works fine, but it now produces blocks faster we can consume them.

- [ ] also p2p/worker.ts is kinda messy, and some of chain/ChainStore.ts as well, rewrite it better some time

- [ ] ok now we put the chain logic on its own worker, and made storage stuff, sync. so now we need to redesign a few things for a sync code
      instead of concurrent async code. which will make us gavin a bit more speed. satoshi client(knots/core) sync time with the same local
      network peer takes 11m40s, rn for use it takes 11m12s, so same finally. but we can make it a bit better.

- [ ] keep spender index on the `txid -> txPointer` kv, so something like `txid -> [txPointer, spenderIndex]`, this is both require less
      reads, and also during spent check we dont have to go look at to the txs blobstore. so current path is
      `txid -> txPointer -> tx(offset to spenderIndex) -> spender` instead it would be like `txid -> [txPointer, spenderIndex]`, we can even
      store the spent status on the kv, but we dont wanna hit on rocks for everything. but we are hitting it already, we might just do it
      tho. might make more sense. anyway once we decouplbe spending stuff from txs in a good way, we can also compress those files. with
      zstd
