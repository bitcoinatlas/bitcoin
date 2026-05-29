# Storage Comparison

Satoshi Client (data) is sourced from
[blockchain.com/charts/blocks-size](https://www.blockchain.com/charts/blocks-size), using the entry strictly before the
block height's timestamp (floor).

BitcoinAtlas (data) is the `txs` directory only. BitcoinAtlas (data+indexes) is the full `data` directory.

| Height | Satoshi Client (data) (MiB) | BitcoinAtlas (data) (MiB) | Saved (MiB) | Saved % | BitcoinAtlas (data+indexes) (MiB) |
| ------ | --------------------------- | ------------------------- | ----------- | ------- | --------------------------------- |
| ~222k  | ~5,685                      | ~5,530                    | ~155        | ~2.7%   | ~5,632                            |
| ~230k  | ~6,984                      | ~6,758                    | ~226        | ~3.2%   | ~7,066                            |
| ~235k  | ~7,858                      | ~7,680                    | ~178        | ~2.3%   | ~7,987                            |
| ~245k  | ~9,223                      | ~8,602                    | ~621        | ~6.7%   | ~8,909                            |
| ~256k  | ~10,618                     | ~9,830                    | ~788        | ~7.4%   | ~10,138                           |
| ~263k  | ~11,488                     | ~10,619                   | ~869        | ~7.6%   | ~10,890                           |
| ~268k  | ~12,215                     | ~11,292                   | ~923        | ~7.6%   | ~11,563                           |
| ~301k  | ~18,910                     | ~17,411                   | ~1,499      | ~7.9%   | ~17,682                           |
