# immediate goal (current)

- [ ] sync to the tip, see if it can reach to the tip. then make the ui, check our timechain see if anything is off.

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
- [ ] another thing about new storage system is, we didnt really make our IndexStore, llm made it. so sometime in the future also rewrite
      that as well.
- [ ] another thing to remember is, in BlobStore, we made open() files per get() call and close open close open close. we made this to make
      multi chunk reads and using seek with concurrency more stable, and simpler. but we can probably have reader pool, and queue if the
      every reader of a chunk is being used. or something like that probably. or maybe something else smarter? idk.

- [ ] ok so we made p2p and block download into its own worker, it works fine, but it now produces blocks faster we can consume them.

- [ ] also p2p/worker.ts is kinda messy, and some of chain/ChainStore.ts as well, rewrite it better some time

- [ ] keep spender index on our `txid -> txPointer` kv, so something like `txid -> [txPointer, spenderIndex]`, this is both require less
      reads, and also during spent check we dont have to go look at to our txs blobstore. so current path is
      `txid -> txPointer -> tx(offset to spenderIndex) -> spender` instead it would be like `txid -> [txPointer, spenderIndex]`, we can even
      store our spent status on our kv, but we dont wanna hit on rocks for everything. but we are hitting it already, we might just do it
      tho. might make more sense. anyway once we decouplbe spending stuff from txs in a good way, we can also compress those files. with
      zstd

- [ ] ok now we put our chain logic on its own worker, and made storage stuff, sync. so now we need to redesign a few things for a sync code
      instead of concurrent async code. which will make us gavin a bit more speed. satoshi client(knots/core) sync time with the same local
      network peer takes 11m40s, rn for us it takes 9m31s, so same finally. but we can make it a bit better.

- [ ] ok few new concerns and thoughts. so normally we were just gonna do domain specific encode/decode to optimize storage, but also using
      the fact that we know that we are always indexing for an explorer, to prevent duplication of indexed data on our timechain. it worked,
      and our indexed chain size is close to satoshi client's size without index, without electrum without mempoolspace, but we have all of
      the indexes as well. which is good. BUT its not as small as we hoped for, so to increase the entropy of our chain we thought we might
      do so general purpose compression like zstd on the reaming data, for example on txs blob store chunks. we already optimized storage
      for cross chunk repating data, and using domain specific packing. so it seems zstd can earn like 20% more across the chunks. so we can
      decompress chunks when they are in use and keep them on the disk as long as they are in use then delete them. but issue with that is
      prevouts can be spending outputs from a wide range of our timechain. so then we need a second utxo storage. which we didnt wanna do
      before. and our compressed timechain would be used purely for reorgs and eletrum endpoints, and explorer. satoshi client `chainstate/`
      dir is 11GB atm. so maybe its fine? worse case someone looks at blocks from different chunks back to back, causing a lot of
      decompression of compressed chunks and deletion of decompressed chunks. we can have a setting like `MAX_DECOMPRESSED_CHUNK_COUNT`or
      something?

- [ ] we might not need spender index, and also therefore IndexStore, we can probably just use rocksdb `txid -> stuff` instead probably. the
      reason we use it before was, previus kv impl was kinda weird and didnt allow us to safely mutate data. rn its safe.

- [ ] as i said before get all the benefits of using sync function inside a worker. we dont need concurrent mentality, and guards anymore. i
      believe we can make this shit at least 2-3x faster.

- [ ] it might be a dumb idea but also think about flushes on a different worker? probably? we used to do concurrent, we already designed
      them to work concurrently before, they might as well be running in parallel maybe? we have `freeze()`, `pin()`, etc. it might work.
      that can more than double our speed. give the ownership of the frozen stage to the worker, and let it flush it. and tell you when its
      done. let's think about blob store for example, we can have a flush worker, that we give buffers, and it just shits it into a file
      with append(), in parallel. while we are doing other shit. ArrayStore is a BlobStore wrapper so that would automatically have the same
      parallel flush() ability. really easy to impl tbh. let me first profile how long each flush takes per store. UPDATE: ok IndexStore
      seem to take almost all of the time, and i dont see a lot of overhead on other stores that we would need to flush it in parallel. I
      can optimize IndexStore, but we are probably gonna elimanate it soon anyway. so yeah.
