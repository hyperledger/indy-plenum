# Storage components
As of now, RocksDB is used as a key-value database for all Storages.

#### 1. Ledger
- The ledger is an ordered log of transactions with each transaction assigned a unique monotonically increasing positive integer called sequence number.
- Sequence numbers start from 1 and then increase by 1 for each new transaction. There are no gaps in sequence numbers. 
- RocksDB is used as a key-value storage where key is the sequence number and value is the transaction.
- A transaction is serialised (currently as MsgPack) before storing in the ledger, more on this in the Serialisation doc.
- Exact format of each transaction can be found in [Indy Node Transactions](https://github.com/hyperledger/indy-node/blob/master/docs/transactions.md).
- Each node hosts several ledgers each identified by a unique id:
  -   Pool Ledger (id is 0): Contains transactions related to pool membership, like joining of new nodes, suspension of existing nodes, changing the IP/port or keys of existing nodes.
  -   Domain Ledger (id is 1): Contains transactions related to the core application logic. Currently it contains NYM transactions. The `indy-node` codebase extends this ledger with other identity transactions.
  -   Config Ledger (id is 2): Contains transactions related to the configuration parameters for which the pool needs to agree, eg. if each node of the pool needs to use the value `5` for a config variable `x`, then this ledger should contain transaction specifying the value of `x` as `5`. The `indy-node` codebase extends this ledger with source code update transactions.
  - More ledgers can be added by plugins.
- Each correct node should have exactly the same transactions for each ledger id.
- A ledger is associated with a [compact merkle tree](https://github.com/google/certificate-transparency/blob/master/python/ct/crypto/merkle.py). 
  - Each new transaction is added to the ledger (log) and is also hashed (sha256) and this hash becomes a new leaf of the merkle tree which also 
results in a new merkle root. Thus for each transaction a merkle proof of presence, called `inclusion proof` or `audit_path` can be created by 
using the root hash a few (`O(lgn)`, `n` being the total number of leaves/transactions in the tree/ledger) intermediate hashes. 
  - Hashes of all the leaves and intermediate nodes of the tree are stored in a `HashStore`, enabling the creating of inclusion proofs and subset proofs. A subset proof 
proves that a particular merkle tree is a subset of another (usually larger) merkle tree; more on this in the Catchup doc.\
  - The `HashStore` has 2 separate storages for storing leaves 
and intermediate nodes, each leaf or node of the tree is 32 bytes. Each of these storages can be a binary file or a key value store. 
The leaf or node hashes are queried by their number. 
- When a client write request completes or it requests a transaction with a particular sequence number from a ledger, 
the transaction is returned with its inclusion proof. 
- States and Caches can be deterministically re-created from the Transaction Log.
- There are 9 storages associated with the Ledgers (3 for each of the ledgers):
  - Pool Ledger:
    - `pool_transactions` (Transaction Log)
    - `pool_merkleLeaves` (Hash Store for leaves)
    - `pool_merkleNodes` (Hash Store for nodes)
  - Domain Ledger:
    - `domain_transactions` (Transaction Log)
    - `domain_merkleLeaves` (Hash Store for leaves)
    - `domain_merkleNodes` (Hash Store for nodes)
  - Config Ledger:
    - `config_transactions` (Transaction Log)
    - `config_merkleLeaves` (Hash Store for leaves)
    - `config_merkleNodes` (Hash Store for nodes)
  
Relevant code:
- Ledger: `plenum/common/ledger.py` and `ledger/ledger.py`
- Compact Merkle Tree: `ledger/compact_merkle_tree.py`
- HashStore: `ledger/hash_stores/hash_store.py`


#### 2. State
- Each Ledger has a State. State is a resulting view (projection) of the ledger data used by Node and Application business logic,
as well as for read requests.
- The underlying data structure of state is the [Merkle Patricia Trie](https://blog.ethereum.org/2015/11/15/merkling-in-ethereum/) used by Ethereum.
- The state can be considered a collection of key value pairs but this collection has some properties of merkle tree like a root hash and 
inclusion proof of the keys, eg. If there are 3 key value pairs `k1-v1`, `k2->v2` and `k3->v3`, then state will have a root hash say `R` 
and a proof can be generated that state with root hash `R` has a key `k1` with value `v1`, similarly for other keys. When a new key is added 
or value of an old key is changed the root hash changes from `R` to `R'`. Note that the order of insertion of keys does not matter, any order of the 
keys will result in the same root hash and same inclusion proof.
- It's possible to get the current value (state) for a key, as well as
    a value from the past (defined by a state root hash). 
- The state is built from a ledger, hence each ledger will usually have a corresponding state. State can be reconstructed from the Ledger.
- There are 3 storages associated with every Ledger:
  - `pool_state`
  - `domain_state`
  - `config_state`

Relevant code:
- State: `state/pruning_state.py`
- Merkle Patricia Trie: `state/trie/pruning_trie.py`

#### 3. Node Status Database
- Auxiliary storage to persist data needed for consensus algorithm
- Storage name: `node_status_db`

#### 4. BLS Multi-Signature Database
- Every transaction for the given time (that is every update of the state)
is signed (using BLS schema) by all nodes during consensus,
and BLS multi-signature is created and stored in this state for every state root hash.
- This is used together with a State Proof as part of replies to read requests.
- Storage name: `state_signature`

Relevant code:
- BlsStore: `plenum/bls/bls_store.py`

#### 5. Request to Transaction Mapping Database
- This database stores the mapping `request_digest -> ledger_id<delimiter>seq_no` in a key value store.
- Each client request is uniquely identified by a `digest`.
- When this request is ordered (consensus successfully completes), it is stored in the ledger and assigned a sequence number.
- One use case is that the client can ask any node to give it the transaction corresponding to a request key `digest`, 
and the node will use this database to get the sequence number and then use the sequence number to get the transaction from the ledger. 
- Storage name: `seq_no_db`
 
Relevant code:
- ReqIdrToTxn: `plenum/persistence/req_id_to_txn.py`

#### 6. Timestamp storage
- This database stores the mapping `timestamp -> state root hash` in a key value store.
- This is used to get the state root hash for the given timestamp to efficiently
    get data from the past.
- Storage name: `state_ts_db`
 
Relevant code:
- StateTsDbStorage: `storage/state_ts_store.py`

#### 7. Identifier Cache Database (in Indy Node)
- This database stores the mapping `DID -> verkey/role` in a key value store.
- It's used for efficient validation and getting verkey for a  DID.
- Storage name: `idr_cache_db`

Relevant code:
- IdrCache: `indy-node/persistence/idr_cache.py`

#### 8. Attribute Database (in Indy Node)
- Raw attributes from ATTRIB transaction are stored in this database.
- The ATTRIB transaction in the Domain Ledger contains the hash of the data only.
- Storage name: `attr_db`

Relevant code:
- AttributeStore: `indy-node/persistence/attribuite_store.py`
    
### Common Storage abstractions
The data structures in the code use some abstractions like a `KeyValueStorage` interface which has a LevelDB 
and RocksDB implementations, as well as file implementations (both single and chunked file storages).

Relevant code:
- `storage/kv_store.py`
- `storage` package
