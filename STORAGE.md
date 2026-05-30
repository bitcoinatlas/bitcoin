# Storage Comparison

**Satoshi Client (data)** figures are sourced from
[blockchain.com/charts/blocks-size](https://www.blockchain.com/charts/blocks-size), using the entry immediately before
each block height's timestamp (floor).

**BitcoinAtlas (data)** refers to the `txs` directory only. **BitcoinAtlas (data+indexes)** is the full `data`
directory, and additionally includes all headers synced to the current tip plus a hashToHeight index — so early on, the
index overhead makes the (data+indexes) column appear disproportionately large.

| Height | Satoshi Client (data) (MiB) | BitcoinAtlas (data) (MiB) | Saved (MiB) | Saved % | BitcoinAtlas (data+indexes) (MiB) |
| -----: | --------------------------: | ------------------------: | ----------: | ------: | --------------------------------: |
|   1200 |                          ~0 |                        ~0 |          ~0 |  ~33.5% |                              ~167 |
|  22400 |                          ~5 |                        ~3 |          ~2 |  ~39.8% |                              ~233 |
|  55200 |                         ~14 |                        ~9 |          ~5 |  ~36.2% |                              ~239 |
|  85600 |                         ~36 |                       ~26 |         ~10 |  ~27.0% |                              ~256 |
| 110400 |                         ~82 |                       ~67 |         ~15 |  ~18.6% |                              ~297 |
| 125200 |                        ~168 |                      ~143 |         ~25 |  ~14.9% |                              ~373 |
| 131600 |                        ~260 |                      ~233 |         ~27 |  ~10.4% |                              ~511 |
| 136800 |                        ~394 |                      ~335 |         ~59 |  ~15.0% |                              ~643 |
| 140000 |                        ~467 |                      ~399 |         ~69 |  ~14.7% |                              ~746 |
| 142800 |                        ~535 |                      ~456 |         ~79 |  ~14.7% |                              ~803 |
| 144000 |                        ~559 |                      ~479 |         ~80 |  ~14.4% |                              ~826 |
| 148400 |                        ~651 |                      ~555 |         ~96 |  ~14.8% |                            ~3,398 |
