Ok so i have totally forgot what i was doing EXACTLY.

i remember converting the ibd to parallel multi-worker stages.

i was kinda thinking a lot about it.

but now dont really remember the exact idea i had, so gonna reinvent it, and apply.

---

so instead of so im not gonna flush everything raw to the disk because we are using indexes to optimize the data. reducing repeating data.

so mean slowdown is BASIC validation and index reads and writes. indexes are already fast enough we just need to make it parallel.

rocksdb bindings already handles the multi worker access, its parallel.

ALSO if posibble workers should produce buffers we can just shit into the end of the files or write to rocks in parallel let the library
handle parallel writes.

of course there is an order to this, so we dont miss anything.

so we get a chunks of data from the p2p worker. and we give these chunks to different workers and they all work together in parallel.

handling each blocks buffer chunk. i think.

so indexes that are happening are:

- txid index
- pubkey index
- using prevout pointer from the txid index

in general we need to check some things a few times (not sure about writes rn):

- first within the worker we should do index checks for data already exists in the back.
- also in the same worker do local index and local check. so we also also see repeating data within the worker
- then we should check stuff in between workers on the main thread at sync point or something???

i think this should be better:

- first on the main thread index everything in the chunks locally.
- then we freeze the index and transfer it to the workers. issue here is we cant send it to all workers because only one worker can own a
  buffer at a time. copying is slow (maybe fast idk). we can use a shared buffer probably? idk what is better or there is something better.
- but issue is that is still main thread bounded but at least index is in memory so maybe faster.
- anyway my idea was since workers now know everything between them they can do full index check within the worker.
- forgot if we were able to do transaction in between workers? my can do snapshots? idk
- writes to normal stores has to be on the main thread for sequential append-only write. BUT workers can just gives us buffers to shit at
  the end of the files. which is fast.
- also all of this has to be atomic.

i think workers single shooting is easier?

but i mean stuff is most likely older than few chunks. so it will probably not be seen first time within the worker job group.

so first parallel check probably would be faster? within the workers.

then if we cant find it in the big index we check other workers and local.

but isnt this kinda backwards? small fast index check should be first?

but i mean its mostly likely will not in it, its for edge cases. so i think yeah correct behevior?

also this way decoding itself is parallel.

---

OH wait i see the old plan in NOTES.md its way better than this
