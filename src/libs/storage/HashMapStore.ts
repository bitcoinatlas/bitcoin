import { StructGeneric } from "@nomadshiba/codec";
import { BlobStore, BlobStoreOptions } from "~/libs/storage/BlobStore.ts";
import { Store } from "~/libs/storage/Store.ts";

/*
	TODO: make the code more clean and simpler.
	- we might think about wrapping blobstore for entries again, maybe or maybe not
	- anything cursor related should be atomic with sab
	- assume there is one writer worker at a time, and multiple readers
	- turn the entries append-only, remove set() we should insert new version at the end.
	- then time to time we can run a compaction to remove old versions, and we can do this in a separate worker.
	- we should have a seperate cursor for last compaction, so we dont scan everything every time we want to compact.
	- compaction should be ran manually with a method. some stores dont require it at all, such as blockhash, txid, spender

	but come to think of it i mean only one that needs it is pubkey which stores the last tx with an output to the pubkey.
	but i mean we can avoid compaction if we stored first, but then linked list has to point forwards which is not truncate safe.
	on the other hand buckets always store the last. Maybe hashmap needs to be two different things?
	i have been thinking about this tbh but always found a deal breaker which i cant remember atm.
	but my main idea was we already have a blobstore, all we need is buckets and a linked list to index it.
	so basically derive the key from the entry, value.
	if we dont combine this with blobstore again, then for compaction to work blobstore needs a splice() method.
	if we are gonna do compaction then this because unsafe to truncate, so if we did compaction newer than then truncate point,
	then we cant just truncate we have to clear and reindex. (which shows the imporantance of derived keys, easier to reindex())
	in a hashmap one key can point to more than one entry, more than one value. so this might make sense in same sense.
	like if we can come up with a structure that doesn't require compaction.
	this requires that we shouldnt store same key multiple times. full key should only be stored once.
	we should build buckets on top of an existing blobstore.
	everything should be connected with pointer linked lists.
	buckets give the heads.
	key should be derived from the actual entry, value.
	ok first point is we should be able to get the key without decoding the whole thing, right?
	so keys should be fixed and cant have a variable size field before it.
	but this totally kills being able to index txs. by pubkey.
	but wait we wanna store a pubkey once, and everytime it repeats we wanna point back to it.
	also electrum requires us to map the hash of pubkey. which can be done without storing the hash i tihink, before getting bucket index hash it, easy.
	ok this sounds like we need a transform link function which goes through every entry, replacing things that you dont want repeting with pointers.
	and also index at the same time.

	maybe its this complex because i wanna abstract it with reuseable Store classes. (probably yes)
	there are different things, when i index pubkey on an output, i want to point back to the original pubkey on the output.
	but while indexing txid, i dont want to store pointer to txid on the entry. i wanna keep txid on the entry.
	because its unique. hmm. so an indexer in blobstore, that can accept unique indexes as well as non-unique indexes.
	so do i want these on blob store or create something new that wraps blob store. probably the latter.
	IndexedBlobStore, that can accept multiple indexes, and can accept unique or non-unique indexes.
	it probably cant accept any codec, maybe only StructCodec shapes because it should be able to differentiate between the fields.
	or have some kind of middleware function that lets us do any kind of indexing logic ever.

	ok idea, each store including hashmap ones should be appendonly only, and truncate safe without compaction. so we can have a compaction.
	we need a IndexedStore that has fields, it can modify fields based on what they are.

	if you have a unique index on a field you dont have to move the value on the field else where. it can stay where at where its.
	if its not unique it has to move it somewhere else and point back to it OR point to the first instance.

	it should basically change the way things are encoded on the disk. by adding linked list or like chaging the shape of things,
		while also managing the buckets and cursors, etc...
		or we can have primitives and build our own stuff around it basically.

	so if non works while maximizing ease and storage.
	we can just say fuck it make a buckets primitive.
	and design a special ChainStore that handles everything.

	since we will not need compaction we can just use the BlobStore, which is more consistent with mmap because of the chunking.
*/

/*
	Recovery:
	we always write new entries ahead of the cursor.
	and during reveal we update the buckets for the new size.
	if during bucket update we crash/fail/powergone/terminate what we can do is.
	so in pin() before reveal() is called, sync() is called first,
	so this makes sure the data is on the disk before revealing it and storeing the cursor.
	so this means after a crash, the entires that were being applied to the buckets still there infront of the cursor.
	so this means we can use those entries and their back/prev pointers to restore the buckets to their previous state.
	by reverse replaying.

	one issue here is after a restart stores dont know their old size, they just get called reveal from 0 to new size.
	so for this to work consistantly Manifest might need to remember soon to be revealed new size, old size, and pin size.

	also like idk how much forward i should use recovery.
	so we might actually have two methods?

	reveal() and persist()

	both take a size value, difference is reveal() just updates the size, decides how much should be revealed.
	and persist() gurannted to be called

	...

	or wait wait i have an idea.

	so we have truncate already right?

	so truncate means undo basically.

	so what can we do is, we first record soon to be revealed size.
	then normal pin size during pin()

	then during recovery, if two sizes are same its fine.
	but if they are not same, we set soon to be reveal first.
	THEN call truncate on it. to tell the store to undo that part securely?
	makes sense?
*/

/** entries per bucket */
export type LoadFactorOptions = {
	target: number;
	maxDrift: number;
};

export type HashMapStoreOptions<Shape extends StructGeneric> = {
	path: string;
	shape: Shape;
	indexes: { fields: (keyof Shape)[]; unique: boolean };
	/** entries per bucket */
	loadFactor: LoadFactorOptions;
	blob: BlobStoreOptions;
};

export class HashMapStore<Shape extends StructGeneric> extends Store implements Disposable {
	public readonly path: string;
	public readonly shape: Shape;

	private entries: BlobStore;

	private constructor(options: HashMapStoreOptions<Shape>) {
		super();
		this.path = options.path;
		this.shape = options.shape;
		this.entries = BlobStore.open(options.blob);
	}

	size(): number {
		throw new Error("Method not implemented.");
	}

	reveal(size: number): void {
		throw new Error("Method not implemented.");
	}

	persist(): void {
		throw new Error("Method not implemented.");
	}

	truncate(size: number): void {
		throw new Error("Method not implemented.");
	}

	sync(): void {
		throw new Error("Method not implemented.");
	}

	close(): void {
		throw new Error("Method not implemented.");
	}

	[Symbol.dispose](): void {
		this.close();
	}
}
