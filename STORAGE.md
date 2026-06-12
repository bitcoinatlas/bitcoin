# Storage Comparison

**Satoshi Client (data)** figures are sourced from
[blockchain.com/charts/blocks-size](https://www.blockchain.com/charts/blocks-size), using the entry immediately before
each block height's timestamp (floor).

**BitcoinAtlas (data)** refers to the `txs` directory only. **BitcoinAtlas (data+indexes)** is the full `data`
directory, which includes all indexes and all headers synced to the current tip — so early on, the index overhead makes
the (data+indexes) column appear disproportionately large relative to (data).

**Indexes:**

- `txid` → byte offset of the transaction in the chain
- `scriptPubKey` → byte offset of the tx output containing the first appearance of the scriptPubKey
- Each output carries a `spent` bit indicating whether it has been consumed

**Planned Query Support:**

> Not necessarily additional indexes — implementation may use tricks to derive these without dedicated structures.

- `scriptPubKey` → `txid[]`
- tx output → spending input (`spentBy`), replacing the current `spent` bit with a full link

| Height | Satoshi Client (data) (MiB) | BitcoinAtlas (data) (MiB) | Saved (MiB) | Saved % | BitcoinAtlas (data+indexes) (MiB) |
| -----: | --------------------------: | ------------------------: | ----------: | ------: | --------------------------------: |
