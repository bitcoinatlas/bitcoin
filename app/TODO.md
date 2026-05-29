- [x] WAL prevents read, defeats purpose of it, shouldnt.
- [ ] in a tick some stuff should work in parallel. sequentially.
- [x] Download should queue blocks in memory, and another loop should append them from a block pool in order.
- [x] use "push" for array store and "append" for blob store
- [x] on stores instead of "transaction" use the term, "batch", so it doesnt become confusing and mixed with bitcoin
      txs.
- [ ] forgot to seperate between txid and wtxid, do that. then fully sync check the full chain size. then fucking
      refactor everything, codebase has kinda become a mess.
- [ ] verify should be seperated complately to its own height meta data, and worker shouldnt effect the main thread and
      storage stuff. so it shouldn't effect raw download and write speed we have without verification.
- [ ] block txs start pointer shouldnt be packed with the header. limits parallelism. instead it should have a seperate
      arraystore like before.
- [ ] put all the sync logic in a simplified way into a single file or directory
- [ ] read ahead should be in get() on blobstore before or after the codec, if left empty stride is used, if stried is
      dynamic fail
- [ ] seperate url params infer to InferINput and InferOutput, so we can use StringInput for inputs.
- [ ] higher level storage for fixed height ranges of block txs data. for on demand block body(txs) downloads.
- [ ] make sync faster (try to batch writes, try to make txid index faster as well, also maybe check what is slower)
- [ ] convert stores to classes for consistency
- [ ] make atomic a wrapper class with multiple stores.
- [ ] downloaded non stop just have a max block pool size.
- [ ] keep blocks as bytes when they are in the pool until you start appending it, decoding makes it take more space
      because V8.
- [ ] on IBD a tick can go on non stop. so make saves based on staged size as well, and also time based.
- [ ] during WAL dont throw, instead await. or dont wait at all if posibble.
- [x] create the pubkey index to use it for pointers. repeating pubkeys stored once, referenced by pointer everywhere.
- [x] maybe dont hide pubkey enum type, and have a method to get the raw thing like prevout, so we dont have to keep
      checking its type over and over again in multiple places.
- [ ] we dont need blockhash index on disk probably, we can index it in memory
- [ ] ETA calculation should be based on remaining bytes (from historical size dataset), not remaining block count.
- [ ] on-demand block range download (not just single block) triggered by frontend requests.
- [ ] on-demand fetched blocks go into a separate higher-level store; main sync job checks cache first before requesting
      from peers.
- [ ] frontend: scrollable block size chart where visual weight maps to actual block weight. click/hold to fast-scroll.
- [ ] support Electrum endpoints.
- [ ] support Satoshi RPC endpoints.
- [ ] rename mempool to txpool everywhere.
- [ ] chunk compression for cold data (LZ4 or similar), with uncompressed cache for hot chunks.
