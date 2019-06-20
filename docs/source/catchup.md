# Ledger Catchup

A node uses a process called `catchup` to sync its ledgers with other nodes. It does this process after 
- Starting up: Any node communicates state of its ledgers to any other node it connects to and learns whether is ahead or behind or at same state as others. 
- During a view change: After a view change starts, nodes again communicate state of their ledgers to other nodes. 
- If it realises that it has missed some transactions: Nodes periodically send checkpoints indicating how many transactions they have processed 
recently, if a node finds out that is missed some txns, it will perform catchup
 (see [Checkpoint-based Catchup Trigger Diagram](diagrams/catchup-trigger.png)).  


### Ledger Catchup Order
Plenum has multiple ledgers (see [Storages](storage.md)): audit, pool, config, domain.
Plugins can register other ledgers if needed.

During catchup procedure, all ledgers are caught up in the following order:
1. Audit ledger - all other ledger are caught up based on the latest state of the audit ledger.
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
          the same state as this node's one, then catchup is complete (it means that no catch-up is actually needed since 
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
     - Step 2: request and apply transactions from other nodes         
        - The transactions are requested from other nodes by equally distributing the load.
        If a node has to catchup 1000 txns and there are 5 other nodes in the network,
         then the node will request 200 txns from each. 
        - The transactions are requested only from the nodes the catching up node is connected to. 
        - The catching up node sends a `CatchupReq` message to other nodes and expects a
         corresponding `CatchupRep`.
        - If the node does not receive sufficient or consistent `CatchupRep`s under a timeout (see `CatchupTransactionsTimeout`),
         it requests missing ones from other (connected) nodes. 
2. Catchup Other Ledgers
     - Step 1: learn how many transaction to catch-up
        - Since audit ledger is already caught up at this step, and it contains the status of every other ledger,
        the last audit ledger transaction is used to learn how many trasnactions every ledger needs to catch-up.
        This guarantees consistency of all ledgers after catch-up, that is all ledgers are caught up till the same value.
        See [Audit Ledger](audit_ledger.md) for more details.
        - If the audit ledger is empty, then the same approach to learn how many transaction to catch-up
        as for audit ledger is used for every other ledger.
      - Step 2: request and apply transactions from other nodes
        - The same as for audit ledger.         
           


### Catchup Actors
The catchup is managed by a class called `LedgerManager` which is a thin facade for the following actors:
- `ClientSeederService`: 
    - performs catchup of clients and other nodes 
    - it processes input `LedgerStatus` and `CatchupReq`, 
    and outputs `LedgerStatus`, `ConsistencyProof` and `CatchupRep`.
    - the service is always active.
- `NodeSeederService`: 
    - performs catchup of other nodes 
    - it processes input `LedgerStatus` and `CatchupReq`, 
    and outputs `LedgerStatus`, `ConsistencyProof` and `CatchupRep`.
    - the service is always active.    
- `NodeLeecherService`: 
    - Performs catchup of this node.
    - Active only at the time of the node catchup.
    - Responsible for catchup of all ledgers in the order mentioned above.
    - `LedgerLeecherService` is responsible for catchup of every ledger:
        - Instantiated for every ledger.
        - `ConsProofService` is responsible for Step 1: learn how many transaction to catch-up.
        - `CatchupRepService` is responsible for Step 2: request and apply transactions from other nodes.
     
### Catchup Callbacks

There is a number of callbacks called during catchup:
- Before each ledger starts catch-up
    - to change Node's state (discovering-synching-synced-etc.)
- After each ledger finishes catch-up
    - to change Node's state (discovering-synching-synced-etc.)
- After a transaction received during catchup is added to the ledger
    - to update states and other indexes and caches (see [Storages](storage.md))
- After all ledgers are caught-up
    - make Node participating 
    - adjust current viewNo and primaries (based on the information from the audit ledger)
    - adjust last ordered 3PC (based on the information from the audit ledger)
    - adjust watermarks 
    - unstash and process messages stashed during catchup
       
### Ordering during Catchup
- Replicas do not generate, send or process any 3PC messages while catchup is in progress
- 3PC messages received during catchup are stashed (in received order) 
- The messages are unstashed and processed afetr catchup is finished
       


### Relevant Code:
- LedgerManager: `plenum/common/ledger_manager.py`
- SeederService: `plenum/server/catchup/seeder_service.py`
- NodeLeecherService: `plenum/server/catchup/node_leecher_service.py`
- LedgerLeecherService: `plenum/server/catchup/ledger_leecher_service.py`
- ConsProofService: `plenum/server/catchup/cons_proof_service.py`
- CatchupRepService: `plenum/server/catchup/catchup_rep_service.py`
- Catchup message types: `plenum/common/messages/node_messages.py`  
- Catchup quorum values: `plenum/server/quorums.py`
