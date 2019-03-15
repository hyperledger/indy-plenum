# Ledger Catchup

A node uses a process called `catchup` to sync its ledgers with other nodes. It does this process after 
- starting up: Any node communicates state of its ledgers to any other node it connects to and learns whether is ahead or behind or at same state as others 
- after a view change: After a view change starts, nodes again communicate state of their ledgers to other nodes. 
- if it realises that it has missed some transactions: Nodes periodically send checkpoints indicating how many transactions they have processed 
recently, if a node finds out that is missed some txns, it will perform catchup
 (see [Checkpoint-based Catchup Trigger Diagram](diagrams/catchup-trigger.png)).  


### Ledger Catchup Order
Plenum has multiple ledgers (see [Storages](storage.md)): audit, pool, config, domain.
Plugins can register other ledgers if needed.

During catchup procedure, all ledgers are caught up in the following order:
1. Audit ledger - all other ledger are caught up based on the latest state of audit ledger.
2. Pool ledger - contains information about the current nodes to connect to and use for the catch-up.
3. Config ledger - may contain configuration parameters affecting catchup of other ledgers. 
4. Domain and other ledgers (at this step all other ledgers can catchup simultaneously) - 
catchup of domain and application specific transactions. 

Steps 1-3 are performed sequentially, which means unless catchup for one ledger is complete, 
catchup for others will not start.
 
### Ledger Catchup Steps
See [Catchup Sequence Diagram](diagrams/catchup-procedure.png).

1. Catchup Audit Ledger
     - Step 1: learn how many transaction to catch-up  
         - Learn the correct state (how many txns, merkle roots) of the ledger
          by using `LedgerStatus` messages from other nodes.
         - If there is a quorum (`Quorums.ledger_status`, `n-f`) of equal `LedgerStatus` messages for
          the same audit state as this node's one, then audit ledger catchup is complete (it means that no catch-up is actually needed since 
          the audit ledger is up to date)
         - Otherwise wait for `ConsistencyProof` messages from other nodes until a timeout.
           When a node receives a `LedgerStatus` and it finds the sending node's ledger behind, 
           it sends a `ConsistencyProof` message. This message is like a
           merkle subset proof of the sending node's ledger and the receiving node's ledger, 
           eg. if the sending node A's ledger has size 20 and merkle root `x` and 
           the receiving node B's size is 30 with merkle root `y`, 
           B sends a proof to A that A's ledger with size 20 and root `x` is a subset of B's ledger with size
           30 and root `y`.
        -  After receiving a `ConsistencyProof`, the node verifies it.
        -  If the node does not receive sufficient or consistent `ConsistencyProof`s under a timeout (see `ConsistencyProofsTimeout`), 
        it requests them explicitly.
        -  Once the node that is catching up has sufficient (`Quorums.consistency_proof`, `f+1`)
         and consistent `ConsistencyProof` messages, it knows how many transactions it needs to catchup.
     - Step 2: request transactions from other nodes         
        - The transactions are requested from other nodes by equally distributing the load.
        If a node has to catchup 1000 txns and there are 5 other nodes in the network,
         then the node will request 200 txns from each. 
        - The transactions are requested only from the nodes the catching up node is connected to. 
        - The node catching up sends a `CatchupReq` message to other nodes and expects a
         corresponding `CatchupRep`.
        - If the node does not receive sufficient or consistent `CatchupRep`s under a timeout (see `CatchupTransactionsTimeout`),
         it requests missing ones from other (connected) nodes. 


# Catchup Actors
The catchup is managed by a object called `LedgerManager`. The `LedgerManager` maintains a `LedgerInfo` object for each ledger and references each ledger by its unique id called `ledger_id`.
When a `ledger` is initialised, `addLedger` method of `LedgerManager` is called; this method registers the ledger with the `LedgerManager`. 
`addLedger` also accepts callbacks which are called on occurence of different events, like before/after starting catchup for a ledger, 
before/after marking catchup complete for a ledger, after adding any transaction that was received in catchup to the ledger.


Relevant code:
- LedgerManager: `plenum/common/ledger_manager.py`
- LedgerInfo: `plenum/common/ledger_info.py`
- Catchup message types: `plenum/common/messages/node_messages.py`  
- Catchup quorum values: `plenum/server/quorums.py`
