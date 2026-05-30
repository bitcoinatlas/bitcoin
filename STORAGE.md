# Storage Comparison

**Satoshi Client (data)** figures are sourced from
[blockchain.com/charts/blocks-size](https://www.blockchain.com/charts/blocks-size), using the entry immediately before
each block height's timestamp (floor).

**BitcoinAtlas (data)** refers to the `txs` directory only. **BitcoinAtlas (data+indexes)** is the full `data`
directory, and additionally includes all headers synced to the current tip plus a hashToHeight index — so early on, the
index overhead makes the (data+indexes) column appear disproportionately large.

| Height | Satoshi Client (data) (MiB) | BitcoinAtlas (data) (MiB) | Saved (MiB) | Saved % | BitcoinAtlas (data+indexes) (MiB) |
| -----: | --------------------------: | ------------------------: | ----------: | ------: | --------------------------------: |
|  23600 |                          ~5 |                        ~3 |          ~2 |  ~40.2% |                              ~233 |
|  56400 |                         ~15 |                       ~10 |          ~6 |  ~36.3% |                              ~240 |
|  86400 |                         ~36 |                       ~26 |         ~10 |  ~26.6% |                              ~257 |
| 110000 |                         ~82 |                       ~66 |         ~16 |  ~19.8% |                              ~296 |
| 125200 |                        ~168 |                      ~143 |         ~25 |  ~14.9% |                              ~373 |
