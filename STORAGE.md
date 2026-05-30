# Storage Comparison

**Satoshi Client (data)** figures are sourced from
[blockchain.com/charts/blocks-size](https://www.blockchain.com/charts/blocks-size), using the entry immediately before
each block height's timestamp (floor).

**BitcoinAtlas (data)** refers to the `txs` directory only. **BitcoinAtlas (data+indexes)** is the full `data`
directory, and additionally includes all headers synced to the current tip — so early on, the index overhead makes the
(data+indexes) column appear disproportionately large.

| Height | Satoshi Client (data) (MiB) | BitcoinAtlas (data) (MiB) | Saved (MiB) | Saved % | BitcoinAtlas (data+indexes) (MiB) |
| -----: | --------------------------: | ------------------------: | ----------: | ------: | --------------------------------: |
|  75600 |                         ~28 |                       ~20 |          ~8 |  ~28.6% |                              ~117 |
| 112800 |                         ~91 |                       ~74 |         ~17 |  ~18.4% |                              ~206 |
| 127600 |                        ~194 |                      ~162 |         ~32 |  ~16.6% |                              ~346 |
