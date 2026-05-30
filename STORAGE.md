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

**Planned Query Support:**

> Not necessarily additional indexes — implementation may use tricks to derive these without dedicated structures.

- `scriptPubKey` → `txid[]`
- tx output → spending input

| Height | Satoshi Client (data) (MiB) | BitcoinAtlas (data) (MiB) | Saved (MiB) | Saved % | BitcoinAtlas (data+indexes) (MiB) |
| -----: | --------------------------: | ------------------------: | ----------: | ------: | --------------------------------: |
|   1600 |                          ~0 |                        ~0 |          ~0 |  ~34.8% |                               ~69 |
|  53200 |                         ~14 |                        ~9 |          ~5 |  ~36.6% |                               ~91 |
|  99200 |                         ~58 |                       ~45 |         ~13 |  ~22.0% |                              ~139 |
| 122400 |                        ~149 |                      ~124 |         ~25 |  ~16.5% |                              ~239 |
| 130800 |                        ~239 |                      ~216 |         ~23 |   ~9.8% |                              ~361 |
| 135600 |                        ~365 |                      ~311 |         ~54 |  ~14.7% |                              ~486 |
| 140400 |                        ~481 |                      ~407 |         ~74 |  ~15.3% |                              ~614 |
