# Audit Ledger
 
## Why It Is Needed
Audit ledger is one of 4 default ledgers.
It can be used for 
- Synchronization between ledgers
- Recovering of pool state by new or restarted nodes
- External audit

#### Synchronization between ledgers
We need this synchronization because of the following:
 
- No global sequence number between ledgers:
    - There are multiple ledgers and states (pool, domain, config, etc.).
    - Transactions are written to the ledgers as a result of global ordering the consensus protocol
    is responsible for.
    - Transactions are ordered in 3PC batches where one batch 
    can contain multiple transactions belonging to the same ledger.
    - However, each ledger has its own sequential ordering so that it doesn't have any information about a 3PC batch 
    a transaction was ordered in.
    - Transactions in ledgers are not independent. Transactions from one ledger are used for validation of 
    transactions from other ledgers during ordering:
        - Domain ledger transactions are used for authentication and authorizations (roles and signatures verification)
        - Pool ledgers transactions represent the current nodes in the pool and their keys, which is used for BLS multi-signature verification
        - Config ledger transactions are used for authentication and authorizations (define what roles and how many signatures are required for every action)
    - So, Audit ledger can be used to restore the global ordering of 3PC batches and hence transactions between all other ledgers.
- Ledgers are caught up sequentially and one by one (see [Catchup](catchup.md))
    - Ledgers are caught-up in the following order: Audit -> Pool -> Config -> Domain.
    - If Pool continues ordering and writing to ledgers while one of the nodes is doing catch-up,
    then this node may have one of the ledgers be ahead after the catch-up. 
    - Since transactions are not independent, that may lead to incorrect validation of transactions the Node needs to apply further. 
    - So, Audit ledger can be used to make sure that all ledgers are caught up to the same point (3PC Batch).
    That's why Audit Ledger is caught-up first.
    
   
 
#### Recovering of pool state

If a Node is restarted and added to the Pool, it needs to know what is the current state of the Pool to 
join consensus protocol.
In particular, it needs to know:
- The current ledgers and corresponding states and caches 
    - Done as a result of catch-up procedure
- Current viewNo
    - Contains in the last transaction from Audit ledger    
- (viewNo, ppSeqNo) of the last ordered 3PC Batch
    - Contains in the last transaction from Audit ledger
- Primaries for each protocol instance         
    - Contains in the last transaction from Audit ledger

#### External audit

Although we have a reliable BFT consensus protocol, one may want to perform external audit and validation of
transactions in all ledgers.
Because of the reasons mentioned in [Synchronization between ledgers](#synchronization-between-ledgers) section, Audit ledger is required for this.
 
With the audit ledger, external audit can be performed as follows:
- Go through transactions in the audit ledger
- Get all transactions from the ledger X corresponding to the audit transaction 
- Apply and validate transactions one by one recovering the ledger X and state X
- Check that ledger and state root hashes from audit transaction are equal to the root hashes of ledger X and state X.


## How It Is Implemented
- Technically Audit Ledger doesn't differ from other ledgers. The only exception is that it doesn't have State.
- Audit Ledger is the first Ledger to catch-up
- Audit ledger has just one transaction type: AUDIT (id=`2`)

#### Audit Transaction Format
```
{
    "ver": "1",
    "txn": {
        "type": "2", # AUDIT
        "protocolVersion": <...>, # CURRENT_PROTOCOL_VERSION

        "data": {
            "ver": "1",
            
            # view no of last 3PC batch which is equal to the current view no
            "viewNo": <...>, 
            
            # ppSeqNo of last 3PC batch
            "ppSeqNo": <...>, 
            
            # a map of ledger_id -> size
            "ledgerSize": { 
                0: <...>, # pool ledger size
                1: <...>, # domain ledger size
                2: <...>, # config ledger size
                1001: <...>, # plugin ledger size
            },
            
            # either a root hash as base58, or a delta, 
            # that is a difference between the current audit txn seqno and 
            # a seq_no of audit txn where it was changed last time
            "ledgerRoot": {  
                0: <...>, # pool ledger root (hash or seqno delta)
                1: <...>, # domain ledger root (hash or seqno delta)
                2: <...>, # config ledger root (hash or seqno delta)
                1001: <...>, # plugin ledger root (hash or seqno delta)
                # -1: <...>, in case of shared root for all ledgers 
            },
            
           # a state root hash as base58 for the ledgers that changed the state in this 3PC
           "stateRoot": {   
                0: <...>, # pool state root hash
                1: <...>, # domain state root hash
                2: <...>, # config state root hash
                1001: <...>, # plugin state root hash
                # -1: <...>, in case of shared root for all ledgers  
            },
            
            # either a list of primaries (Node names) for every protocol insatnce, 
            # or a delta to the audit transaction the primaries were changed last time
            "primaries": <...>    
        },
        "metadata": {
        },
    },
    "txnMetadata": {
        "txnTime": <...>,
        "seqNo": <...>,
    },
    "reqSignature": {
    }
} 
```
- Please note, that `ledgerRoot` and `stateRoot` have values for all ledgers (or one `-1` value in future once we have a combined root).
- `ledgerRoot` value for every ledger can be either
   - ledger root hash as base58, if this ledger was changed in the 3PC batch the audit txn is created for
   - delta, that is a difference between the current audit txn seq_no and a seq_no of audit txn where it was changed last time
- `primaries` value can be either
    - a list of primaries (Node names) for every protocol instance where the Node name is Node's `alias` from the NODE txn. 
    Example: `['Alpha', 'Beta', 'Gamma']`.
    - delta, that is a difference between the current audit txn seq_no and a seq_no of audit txn where the primaries were changed last time
- The deltas are used to have more compact data than if we stored all root hashes all the time (even if it's not changed).
- If the ledger is never changed, then the corresponding `ledger_id` is not present at all.


#### Audit Transaction Ordering
- Audit ledger txn is created for every 3PC batch including freshness updates.
- In order to have up-to-date information about new primaries for every protocol instance after 
the primaries are re-selected (as a result of view change or nodes addition/demotion/promotion),
a master Primary sends a 3PC Batch immediately after re-selection so that 
a new audit transaction with the new primaries is written to the audit ledger.
- So, for every Pre-prepare (3PC batch containing N transactions) transactions are applied to two ledgers:
    - ledger a 3PC Batch was created for (N transactions a Pre-prepare was created for)
    - audit ledger (one audit ledger transaction)
- Pre-prepare contains root hash of the audit ledger after applying audit transaction, 
so that other replicas also create and apply the audit transaction and compare their root hash with the one 
received in the Pre-prepare. It guarantees consistency of data in the audit ledger the same way as consistency 
of data is guaranteed in other ledgers.
- Audit ledger transaction is committed at the same time as transactions associated with the 3PC Batch are committed.
  

## Where It Is Used

#### Catchup
- Audit ledger is caught up first.
- The last transaction in the audit ledger defines a 3PC Batch we are going to catch-up other ledgers to.
- The size of every ledger is taken from the last audit transaction and compared with the current size, so that
the node knows how many transaction it needs to cacth-up for every ledger.
- If ordering and writing to ledgers is in progress when the node is catching up, then 
it will stash all corresponding 3PC messages and will apply them when catch-up finishes. Since 
all ledgers are correctly caught up till the same 3PC Batch and the pool state is properly recovered (see next section),
the node can achieve the same state as other nodes.
 
#### Pool State Recovering 
The last audit ledger transaction contains information about 
 - current `viewNo`
 - last ordered `(viewNo, ppSeqNo)`
 - primaries for every protocol instance
 
So, a node can restore this information immediately from the last audit transaction after catch-up. 
 
 