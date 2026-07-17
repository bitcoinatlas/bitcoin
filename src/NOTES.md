## 3

ok so its working fast now. so few things. we need to index spenders, we are not doing that rn.

then after that, we need to sepearete ibd sync logic from ChainStore. ChainStore should be all about reading from the disk.

so for reads we should directly read from disk. to not read partial data we should limit our reads to the pins.

reason we do this way is chain worker might not always be able to answer us if we are using messages.

tho maybe we should include async options and use async calls on the main chain worker? so it can respond to us.

but we dont need that all, we can directly read from the disk.

any seperate stuff from ChainStore, make it a read wrapper maybe?

this also means dont make `atomic` global. maybe rename it to something else as well?

idk. or it can all be global including the readers/viewers. why not.

then we can delete ChainStore.ts probably as well.

...

syncing to the tip before i do anything else (syncing+indexing):

```
[chain] round 254 | blocks=38 txs=66882 height=687292/958399 | overall 4536 tx/s · current 4722 tx/s · 2.5 blk/s | remaining=271107 ETA 1d5h
```

---

ok here is the idea. so we have 80-90% cpu usage spikes across cores, most heavy work is the `init` stage, but between `init` and `process`
there is a huge gap between spikes where main-chain-thread is busy mostly, a sync point. so the spender indexing workers can work in between
that time. so when the `init` stage ends before doing the main-chain-thread work we should start the spender index workers. one important
thing is it should process the data from the previous round that is already done.

---

actually i dont need transaction/batch for spender index.

because i can keep track of what i did last successfully.

then later if it fails i would just override "put" again right?

like i can have parallel rounds. and after procssing like block 10-20 successfully i would say i did up-to 20 succeesfully.

then if it fails between 20-30

on restart i would do 20-30 again. override the old ones.

right?

so it can work in parallel in rounds without huge sync points.

so in parallel we can still detect double spending right?

yes we can use `noOverwrite`, and if it fails (which should be rare on terminations and stuff) we can get the actual value and check if its
already what we are trying to set it to, or if its something different and valid. so this gurantees double spend check in parallel, without
batching(transaction) which requires a single worker.

---

hoped order of things:

- impl spender indexer (index spenders + check for double spending)
- run with indexer
- clean up code seperate things.
- re-enable api, frontend and gui. (look into the new deno desktop, which might simplify what we are doing)
- impl on-demand blocks data
- at this point work on the frontend, make it nice, easy to read etc. having a frontend also makes it easier to debug data.
- mainly focus on frontend now. make it nice useful have some useful metrics and shit, make it a nice explorer.
- maybe speed it up more on some obvious places
- impl compression on BlobStore. enable it for txs and pubkeys, this should move us to 500GB range for everything (full chain data + full
  indexes)
- impl script verification workers. (can be complex, or easy idk, try to just run it first, also try to convert it to wasm maybe, and stuff
  findout what is faster, probably just running is faster), during ibd try to do it on idle times where we are not using all cores. dont
  slow down the ibd. (we can probably abstract this kind of work, like we can run the workers before main thread sync point work, then tell
  it to pause before running ibd workers again, same as spender indexing workers, dont slow down the ibd, use the sync points to run things
  that can run in parallel independtly)
- when everything is nice, faster, clean, easy to reason, read, understand, start implimenting relay stuff and other things.
- then impl electrum, and satoshi rpc endpoints, this one should be easy when we have everything. probably.
- then i guess 0.0.1 is ready, probably.

## 2

anyway so new goal, parallesim. since we dont have 300KB blocks around 500k we slowdown a lot. so we have to squize everything, cant afford
to be sloppy.

so basically we have p2p waiting for chain worker to consume its chunks constantly. takes a lot of time. so we are just gonna have more
chain workers consuming more chunks in parallel.

so we are not gonna race to process chunks.

instead we are gonna have some workers, running in parallel then syncing in the end.

so first of all we need to strip concurrent stuff from the blobstore, no freeze(), or a stage, we dont even need the batch, all we need is
pin() and rollback().

so each worker started to work on their chunk with multiple blocks and multiple txs. chunk size might be dynamic later based on how fast we
are consuming the incrise parellelsim.

anyway what do these workers do?

they loop blocks, txs, outputs, inputs ok.

and in memory within that worker, and also on the disk they check if scriptpubkey repeats, they also search for prevout tx. if they cant
find it in during the worker time, they check it in the sync time as well in the end.

they also check spent status of the prevout if they can find it during worker or sync time.

simple.

at the same time they create few buffer, one buffer is for data that will not shift the layout if we find a pointer. another buffer for
things that might change if we find a pointer during sync time.

so we can just dumb the buffer that is not gonna change directly to the disk raw, fast.

so before we had a one big blob for all of the txs ever, and we had a pointer pointing at stuff in it we had a packed 3bit flag on the tx
output telling us if the script pubkey is raw, a known format, or a pointer. but thats layout shift.

instead now we are gonna have a pointer always. but this pointer isnt pointing back on one big chain blob. but a blob made out of packed
unique scriptpubkeys appended back to back. so pointer points to here. this gives us parallesim. because there is no shift in the layout
based on back data.

we also do the same thing for prevouts.

so scriptpubkey and prevout tx, if we find those during worker time we dont have to double check it sync time.

but for spent pointers we have to double check them all the time. but during sync time we just double check the other worker's memory
results. so it should be fast.

maybe rocksdb might help us during check as well? based on the impl of bindings we have. i gotta check that. we might not even need sync
time check.

also i think rocksdb bindings we use might be loving concurrency, because its actually paralllesim under the hood. so keep that in mind as
well.

also i just realized we can open the rocksdb on different workers and can even open it with different family columns. bindings just use the
same handle under the hood.

anyway so i think that was all. hopefully it will let us consume chunks p2p worker giving us faster and will drain it all.

## 1

- sync to the tip, see if it can reach to the tip. then make the ui, check our timechain see if anything is off.

- then focus on more speed + storage optimizations.

- more stable peer communication interface.

- clean up the codebase.

- impl on-demand block downloads.

- make it look cool. configs and stuff.

- then do relay p2p stuff

## 0

- turn it into a real app with real ui. so things start to fall into their place. instead of looking like a one big sync script. (makkeei
  titncoool sas s fuckck llsks)

- ok the new storage system we made works right rn. need some more recactor to refine the like flow and interfaces better. but looks good
  rn. another thing is. with BlobStore we call fsync too much. so while refining the interface think about it ,probably make it happen at
  the end of the flush only. i thinking something like dirty flag for the chunks. so we know what to fsync.
- another thing about new storage system is, we didnt really make our IndexStore, llm made it. so sometime in the future also rewrite that
  as well.
- another thing to remember is, in BlobStore, we made open() files per get() call and close open close open close. we made this to make
  multi chunk reads and using seek with concurrency more stable, and simpler. but we can probably have reader pool, and queue if the every
  reader of a chunk is being used. or something like that probably. or maybe something else smarter? idk.

- ok so we made p2p and block download into its own worker, it works fine, but it now produces blocks faster we can consume them.

- also p2p/worker.ts is kinda messy, and some of chain/ChainStore.ts as well, rewrite it better some time

- keep spender index on our `txid -> txPointer` kv, so something like `txid -> [txPointer, spenderIndex]`, this is both require less reads,
  and also during spent check we dont have to go look at to our txs blobstore. so current path is
  `txid -> txPointer -> tx(offset to spenderIndex) -> spender` instead it would be like `txid -> [txPointer, spenderIndex]`, we can even
  store our spent status on our kv, but we dont wanna hit on rocks for everything. but we are hitting it already, we might just do it tho.
  might make more sense. anyway once we decouplbe spending stuff from txs in a good way, we can also compress those files. with zstd

- ok now we put our chain logic on its own worker, and made storage stuff, sync. so now we need to redesign a few things for a sync code
  instead of concurrent async code. which will make us gavin a bit more speed. satoshi client(knots/core) sync time with the same local
  network peer takes 11m40s, rn for us it takes 9m31s, so same finally. but we can make it a bit better.

- ok few new concerns and thoughts. so normally we were just gonna do domain specific encode/decode to optimize storage, but also using the
  fact that we know that we are always indexing for an explorer, to prevent duplication of indexed data on our timechain. it worked, and our
  indexed chain size is close to satoshi client's size without index, without electrum without mempoolspace, but we have all of the indexes
  as well. which is good. BUT its not as small as we hoped for, so to increase the entropy of our chain we thought we might do so general
  purpose compression like zstd on the reaming data, for example on txs blob store chunks. we already optimized storage for cross chunk
  repating data, and using domain specific packing. so it seems zstd can earn like 20% more across the chunks. so we can decompress chunks
  when they are in use and keep them on the disk as long as they are in use then delete them. but issue with that is prevouts can be
  spending outputs from a wide range of our timechain. so then we need a second utxo storage. which we didnt wanna do before. and our
  compressed timechain would be used purely for reorgs and eletrum endpoints, and explorer. satoshi client `chainstate/` dir is 11GB atm. so
  maybe its fine? worse case someone looks at blocks from different chunks back to back, causing a lot of decompression of compressed chunks
  and deletion of decompressed chunks. we can have a setting like `MAX_DECOMPRESSED_CHUNK_COUNT`or something?

- we might not need spender index, and also therefore IndexStore, we can probably just use rocksdb `txid -> stuff` instead probably. the
  reason we use it before was, previus kv impl was kinda weird and didnt allow us to safely mutate data. rn its safe.

- as i said before get all the benefits of using sync function inside a worker. we dont need concurrent mentality, and guards anymore. i
  believe we can make this shit at least 2-3x faster.

- it might be a dumb idea but also think about flushes on a different worker? probably? we used to do concurrent, we already designed them
  to work concurrently before, they might as well be running in parallel maybe? we have `freeze()`, `pin()`, etc. it might work. that can
  more than double our speed. give the ownership of the frozen stage to the worker, and let it flush it. and tell you when its done. let's
  think about blob store for example, we can have a flush worker, that we give buffers, and it just shits it into a file with append(), in
  parallel. while we are doing other shit. ArrayStore is a BlobStore wrapper so that would automatically have the same parallel flush()
  ability. really easy to impl tbh. let me first profile how long each flush takes per store. UPDATE: ok IndexStore seem to take almost all
  of the time, and i dont see a lot of overhead on other stores that we would need to flush it in parallel. I can optimize IndexStore, but
  we are probably gonna elimanate it soon anyway. so yeah. Ok so im gonna remove IndexStore btu first let me sync to the segwit, im almost
  there, let's first see if there are any issues there first.
