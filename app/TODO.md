- [ ] WAL prevents read, defeats purpose of it, shouldnt.
- [ ] in a tick some stuff should work in parallel. sequentially.
- [ ] Download should queue blocks in memory, and another loop should append them from a block pool in order.
- [ ] use "push" for array store and "append" for blob store
- [ ] on stores instead of "transaction" use the term, "batch", so it doesnt become confusing and mixed with bitcoin
      txs.
- [ ] verify should be seperated complately to its own height meta data, and worker shouldnt effect the main thread and
      storage stuff.
- [ ] block txs start pointer shouldnt be packed with the header. limits parallelism. instead it should have a seperate
      arraystore like before.
- [ ] put all the sync logic in a simplified way into a single file or directory
