# notes

- rocksdb bindings we use dont support bloom filters yet, so just wait for an update for it. or you might change
  bindings as well.
- clean up blobstore to be more readable and better.
- clean up chain.ts instead turn it into a one nice class you can use.
- get rid of remaning Tx, TxInput, TxOutput etc classes in favor of pure Codecs, and pure helper functions.
- use namespaces in the backend, makes it more organized. its backend app code we dont have to worry about tree shaking.
  probably just export as object.
- turn it into a real app with real ui. so things start to fall into their place. instead of looking like a one big sync
  script.
