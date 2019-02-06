# Ledger catchup

A node uses a process called `catchup` to sync its ledgers with other nodes. It does this process after 
- starting up: Any node communicates state of its ledgers to any other node it connects to and learns whether is ahead or behind or at same state as others 
- after a view change: After a view change starts, nodes again communicate state of their ledgers to other nodes. 
- if it realises that it has missed some transactions: Nodes periodically send checkpoints indicating how many transactions they have processed 
recently, if a node finds out that is missed some txns, it will perform catchup.  

The catchup is managed by a object called `LedgerManager`. The `LedgerManager` maintains a `LedgerInfo` object for each ledger and references each ledger by its unique id called `ledger_id`.
When a `ledger` is initialised, `addLedger` method of `LedgerManager` is called; this method registers the ledger with the `LedgerManager`. 
`addLedger` also accepts callbacks which are called on occurence of different events, like before/after starting catchup for a ledger, 
before/after marking catchup complete for a ledger, after adding any transaction that was received in catchup to the ledger.
The `LedgerManager` performs catchup of each ledger sequentially, which means unless catchup for one ledger is complete, catchup for other will not start. 
This is mostly done for simplicity and can be optimised but the pool ledger needs to be caught up first as the pool ledger knows how many nodes are there 
in the network. Catchup for any ledger involves these steps:
-   Learn the correct state (how many txns, merkle roots) of the ledger by using `LedgerStatus` messages from other nodes.
-   Once sufficient (`Quorums.ledger_status`) and consistent `LedgerStatus` messages are received so that the node knows the latest state of the ledger, if it finds it ledger to be 
latest, it marks the ledger catchup complete otherwise wait for `ConsistencyProof` messages from other nodes until a timeout.
-   When a node receives a `LedgerStatus` and it finds the sending node's ledger behind, it sends a `ConsistencyProof` message. This message is like a 
merkle subset proof of the sending node's ledger and the receiving node's ledger, eg. if the sending node A's ledger has size 20 and merkle root `x` and 
the receiving node B's size is 30 with merkle root `y`, B sends a proof to A that A's ledger with size 20 and root `x` is a subset of B's ledger with size 
30 and root `y`.
-   After receiving a `ConsistencyProof`, the node verifies it.
-   Once the node that is catching up has sufficient (`Quorums.consistency_proof`) and consistent `ConsistencyProof` messages, it knows how many transactions it needs to catchup and 
then requests those transactions from other nodes by equally distributing the load. eg if a node has to catchup 1000 txns and there are 5 other nodes in the 
network, then the node will request 200 txns from each. The node catching up sends a `CatchupReq` message to other nodes and expects a corresponding `CatchupRep`

Apart from this if the node does not receive sufficient or consistent `ConsistencyProof`s under a timeout, it requests them using `request_CPs_if_needed`.
Similarly if the node does not receive sufficient or consistent `CatchupRep`s under a timeout, it requests them using `request_txns_if_needed`. 
These timeouts can be configured using `ConsistencyProofsTimeout` and `CatchupTransactionsTimeout` respectively from the config file


Relevant code:
- LedgerManager: `plenum/common/ledger_manager.py`
- LedgerInfo: `plenum/common/ledger_info.py`
- Catchup message types: `plenum/common/messages/node_messages.py`  
- Catchup quorum values: `plenum/server/quorums.py`
