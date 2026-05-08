## Compression Methods

**Main methods:**

- Deflate repeating data
- Better data types
- Even use bits when you can
- Flag known patterns.

## Stores

All store mutations should be staged on memory first. If you wanna apply your changes to disk, you use WAL.

There for this also makes txs in memory as well, because all mutations are in memory. Transactions are just a way of
making all or none changes in memory.
