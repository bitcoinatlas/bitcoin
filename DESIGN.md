# Structs & Traits Design

## Structs

### Wire Protocol Primitives (`satoshi/primitives/`)

Already mostly plain types — minimal changes needed.

```
BlockHeader = { version, prevBlock, merkleRoot, timestamp, bits, nonce, hash }
TxIn        = { prevOut: { txId: Bytes32, vout: number }, sequence, scriptSig, witness }
TxOut       = { value: bigint, scriptPubKey: Uint8Array }
Tx          = { txId: Bytes32, version, lockTime, inputs: TxIn[], outputs: TxOut[] }
Block       = { header: BlockHeader, txs: Tx[] }
```

### Weirdness (discriminated unions)

```
SequenceLock = { tag: "final" } | { tag: "disable" } | { tag: "enable", type, value }
TimeLock     = { tag: "none" } | { tag: "height", height } | { tag: "time", timestamp }
```

### Low-Level Primitives

```
Bytes32      = Uint8Array (32 bytes)
StoredPointer = number (u48, 6-byte global file offset)
```

### Chain Domain

```
ChainNode  = { header: BlockHeader, height: number, chainWork: bigint }
Chain      = { nodes: ChainNode[] }
ChainState = { chains: Chain[], active: Chain }
BlockPointer = { file: number, offset: number }   // if we split the u48 into parts
```

### P2P Domain

```
PeerInfo     = { ip: string, port: number, services: bigint }
PeerConnection = { conn: Deno.TcpConn, info: PeerInfo, reader: ReadableStream, writer: WritableStream }
PeerPool     = { peers: Map<string, PeerConnection>, seeds: PeerSeed[] }
PeerMessageEnvelope = { command: string, payload: Uint8Array, checksum: Uint8Array }
```

### Storage: Unsettled vs Settled

A block goes through two storage representations:

**Unsettled** — downloaded from the wire but not yet anchored in the store. References use
raw txIds because the referenced transactions may not have a stable position yet.

```
UnsettledTxInput  = { prevOut: { txId: Bytes32, vout: u24 }, sequence, scriptSig, witness }
UnsettledTx       = { txId: Bytes32, version, lockTime, inputs: UnsettledTxInput[], outputs: StoredTxOutput[] }
UnsettledBlock    = { header: BlockHeader, coinbase: StoredCoinbaseTx, txs: UnsettledTx[] }
```

**Settled** — validated, indexed, and all references resolved to global file pointers (u48).

```
SettledTxInput  = { prevOut: { tx: StoredPointer, vout: u24 }, sequence, scriptSig, witness }
SettledTx       = { txId: Bytes32, version, lockTime, inputs: SettledTxInput[], outputs: StoredTxOutput[] }
SettledBlock    = { header: BlockHeader, coinbase: StoredCoinbaseTx, txs: SettledTx[] }
```

**StoredTxOutput** stays the same in both — it's self-contained (bit-packed header + payload).
The `pointer` scriptType on outputs is for deduplication of identical scripts, not for
referencing other transactions.

```
StoredTxOutput = { value: bigint, spent: boolean, scriptType: "pointer", pointer: StoredPointer }
               | { value: bigint, spent: boolean, scriptType: "raw", scriptPubKey: Uint8Array }

StoredCoinbaseTx = { version, lockTime, coinbase: Uint8Array, outputs: StoredTxOutput[] }
```

### Witness Compression (existing, stays as codec)

```
StoredWitness = enum {
  empty, p2wpkh, p2wpkhMalleated, p2tr_keypath, p2tr_keypath_annex,
  p2tr_script_1, p2tr_script_2, p2tr_script_3, p2tr_script_4,
  p2wsh_multisig_1of1 .. p2wsh_multisig_3of3,
  raw
}
```

---

## Traits

### `Hashable<Self>`

Compute the canonical hash of a structure.

```
Hashable<Self> = {
  hash(self: Self): Bytes32
}
```

Applies to: `BlockHeader` (double-SHA256 of 80-byte header), `Tx` (txid via non-witness
serialization), `Block` (via its header).

### `MessageHandler<Self>`

Handle an incoming peer message. Already close to this shape in existing handler files.

```
MessageHandler<Self> = {
  command: string
  handle(self: Self, peer: PeerConnection, payload: Uint8Array): Promise<void>
}
```

### `Chainable<Self>`

Validate and extend a chain with a new header.

```
Chainable<Self> = {
  validate(self: Self, prev: ChainNode): boolean
  extend(self: Self, header: BlockHeader): ChainNode
}
```

Applies to: `Chain`.

### `Settleable<Self, Settled>`

Convert an unsettled structure to its settled form once all references can be resolved.

```
Settleable<Self, Settled> = {
  settle(self: Self, resolve: (txId: Bytes32) => StoredPointer): Settled
}
```

Applies to: `UnsettledTxInput → SettledTxInput`, `UnsettledTx → SettledTx`,
`UnsettledBlock → SettledBlock`. Settling is all-or-nothing at the block level — if any
input can't resolve, the whole block stays unsettled.

### `Persistable<Self>`

Save and load a structure to/from disk.

```
Persistable<Self> = {
  save(self: Self, path: string): Promise<void>
  load(path: string): Promise<Self>
}
```

Applies to: `ChainState` (header chain file), `HeightIndex` (flat indexed file).

### `Iterable<Self, Item>`

Traverse a structure's elements.

```
Iterable<Self, Item> = {
  iter(self: Self): Iterator<Item>
  get(self: Self, index: number): Item | undefined
  len(self: Self): number
}
```

Applies to: `Chain` (iterate/index `ChainNode`s by height).

### `Downloadable<Self>`

Fetch blocks from peers with retry across multiple connections.

```
Downloadable<Self> = {
  download(self: Self, hashes: Bytes32[]): AsyncGenerator<Block>
  retry(self: Self, hash: Bytes32): Promise<Block>
}
```

Applies to: block downloading (currently `BlockDownloader`).

### `Connectable<Self>`

Manage a TCP connection to a Bitcoin peer.

```
Connectable<Self> = {
  connect(self: Self, addr: PeerInfo): Promise<Self>
  disconnect(self: Self): void
  send(self: Self, msg: PeerMessageEnvelope): Promise<void>
}
```

Applies to: `PeerConnection`.
