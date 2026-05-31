- [x] WAL prevents read, defeats purpose of it, shouldnt.
- [ ] in a tick some stuff should work in parallel. sequentially.
- [x] Download should queue blocks in memory, and another loop should append them from a block pool in order.
- [x] use "push" for array store and "append" for blob store
- [x] on stores instead of "transaction" use the term, "batch", so it doesnt become confusing and mixed with bitcoin
      txs.
- [x] forgot to seperate between txid and wtxid, do that.
- [x] blob store read tries to read from disk even if the data is in the stage only rn and not flushed. fix it. uses
      `getFromDiskWithCodec` while it should read from staged.
- [x] KV shard growth is not atomic, is not failsafe, doesnt survive power outage or termination. fix it.
- [ ] then fully sync check the full chain size. then fucking refactor everything, codebase has kinda become a mess.
- [ ] need a better kv impl
- [x] for blob store, staged can just be a big uint8array probably. or "parts" of it. would make reading from staged
      easier to handle. then updates can also update on those parts etc... like dynamic sized small chunks in memory.
- [ ] blobstore need patch.

---

- [ ] output needs `spentBy` pointing to the spender tx with u48, then we can find the input in that tx.
- [ ] we need a linked list for inputs and outputs. every input or output
- [ ] dynamic things like `spentBy` or or linked list links can be stored on the txid kv? maybe??? in that case we dont
      have to do the "blobstore need patch" task above.

```ts
[value: U51]
[scriptTypeId: U4]	// OP_RETURN shouldn't have rest after payload
[scriptpayload]
[spentBy: U48]     	// which tx spent this output (find input O(N))
[deposit_prev: U48]	// prev output with same scriptpubkey
[deposit_next: U48]	// next output with same scriptpubkey
```

```ts
[...existing_struct...]
[withdraw_prev: U48]     // prev input spending same scriptpubkey
[withdraw_next: U48]     // next input spending same scriptpubkey
```

```ts
spk → [deposit_head: U48, deposit_tail: U48, withdraw_head: U48, withdraw_tail: U48]
```

`deposit_head` has the real data scriptpubkey, no pointer.

An alternative might look like this:

```ts
spk → [deposit_head: U48]
```

since `deposit_head` points to the raw scriptpubkey, within it we can inlude the rest of:

```ts
[deposit_tail: U48, withdraw_head: U48, withdraw_tail: U48]
```

but i dont think that is gonna change much

- [ ] we shouldnt pause everything during flush, takes too much time. double buffer stage or something?? probably.
- [ ] hand refactor the code clean up before making a real frontend ui
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
- [ ] during WAL dont throw, instead await. or dont wait at all if posibble. atomic flush should be able to run while
      the code is still running, we shoulnt wait for it. adds a lot of overhead
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

```
```
