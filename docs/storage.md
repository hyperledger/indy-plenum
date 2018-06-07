# Storage components

#### 1. Ledger
The ledger is an ordered log of transactions with each transaction assigned a unique monotonically increasing positive integer called sequence number.
Sequence numbers start from 1 and then increase by 1 for each new transaction. There are no gaps in sequence numbers. 
The actual storage behind the ledger can be flat files with each transaction in a new line or a key value store like LevelDB with key being the sequence number and value being the transaction.
A transaction is serialised (currently as MsgPack) before storing in the ledger, more on this in the Serialisation doc.
Each node hosts several ledgers each identified by a unique id:
-   Pool Ledger (id is 0): Contains transactions related to pool membership, like joining of new nodes, suspension of existing nodes, changing the IP/port or keys of existing nodes.
-   Domain Ledger (id is 1): Contains transactions related to the core application logic. Currently it contains NYM transactions. The `indy-node` codebase extends this ledger with other identity transactions.
-   Config Ledger (id is 2): Contains transactions related to the configuration parameters for which the pool needs to agree, eg. if each node of the pool needs to use the value `5` for a config variable `x`, then this ledger should contain transaction specifying the value of `x` as `5`. The `indy-node` codebase extends this ledger with source code update transactions.

More ledgers can added by plugins. Each correct node should have exactly the same transactions for each ledger id.

A ledger is associated with a [compact merkle tree](https://github.com/google/certificate-transparency/blob/master/python/ct/crypto/merkle.py). 
Each new transaction is added to the ledger (log) and is also hashed (sha256) and this hash becomes a new leaf of the merkle tree which also 
results in a new merkle root. Thus for each transaction a merkle proof of presence, called `inclusion proof` or `audit_path` can be created by 
using the root hash a few (`O(lgn)`, `n` being the total number of leaves/transactions in the tree/ledger) intermediate hashes. 

Hashes of all the leaves and intermediate nodes of the tree are stored in a `HashStore`, enabling the creating of inclusion proofs and subset proofs. A subset proof 
proves that a particular merkle tree is a subset of another (usually larger) merkle tree; more on this in the Catchup doc. The `HashStore` has 2 separate storages for storing leaves 
and intermediate nodes, each leaf or node of the tree is 32 bytes. Each of these storages can be a binary file or a key value store. 
The leaf or node hashes are queried by their number. When a client write request completes or it requests a transaction with a particular sequence number from a ledger, 
the transaction is returned with its inclusion proof. 

Relevant code:
- Ledger: `plenum/common/ledger.py` and `ledger/ledger.py`
- Compact Merkle Tree: `ledger/compact_merkle_tree.py`
- HashStore: `ledger/hash_stores/hash_store.py`


#### 2. State
The state can be considered a collection of key value pairs but this collection has some properties of merkle tree like a root hash and 
inclusion proof of the keys, eg. If there are 3 key value pairs `k1-v1`, `k2->v2` and `k3->v3`, then state will have a root hash say `R` 
and a proof can be generated that state with root hash `R` has a key `k1` with value `v1`, similarly for other keys. When a new key is added 
or value of an old key is changed the root hash changes from `R` to `R'`. Note that the order of insertion of keys does not matter, any order of the 
keys will result in the same root hash and same inclusion proof. The underlying data structure of state is the [Merkle Patricia Trie](https://blog.ethereum.org/2015/11/15/merkling-in-ethereum/) used by Ethereum.
The state is built from a ledger, hence each ledger will usually have a corresponding state. State can be reconstructed from the Ledger.

Relevant code:
- State: `state/pruning_state.py`
- Merkle Patricia Trie: `state/trie/pruning_trie.py`


#### 3. Seq No database
Each client request is uniquely identified by a `digest`. When this request is ordered (consensus successfully completes),
it is stored in the ledger and assigned a sequence number. This database stores the mapping `digest -> leger_id<delimiter>seq_no` in a key value store.
One use case is that the client can ask any node to give it the transaction corresponding to a request key `digest`, the node will
then use this database to get the sequence number and then use the sequence number of get the transaction from the ledger. 
This database is currently maintained only for domain ledger.
 
Relevant code:
- ReqIdrToTxn: `plenum/persistence/req_id_to_txn.py`


#### Common Storage abstractions
The data structures used in the code use some abstractions like a `KeyValueStorage` interface which has a LevelDB implementation 
`KeyValueStorageLeveldb` and a planned RocksDB implementation, a `SingleFileStore` interface which has a `TextFileStore` and `BinaryFileStore`, 
a `KeyValueStorageFile` interface with a `ChunkedFileStore` implementation and a `DirectoryStore`

Relevant code:
- KeyValueStorage: `storage/kv_store.py`
- KeyValueStorageLeveldb: `storage/kv_store_leveldb.py`
- KeyValueStorageFile: `storage/kv_store_file.py`
- SingleFileStore: `storage/kv_store_single_file.py`
- BinaryFileStore: `storage/binary_file_store.py`
- TextFileStore: `storage/text_file_store.py`
- ChunkedFileStore: `storage/chunked_file_store.py`
- DirectoryStore: `storage/directory_store.py`
