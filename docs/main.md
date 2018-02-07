# Overview of the system

- The system maintains a replicated ordered log of transactions called the ledger. 
- Participants of the system which maintain this log are called the nodes. The nodes run a consensus protocol ([RBFT](http://lig-membres.imag.fr/aublin/rbft/report.pdf)) to agree on the order of transactions. For simplicity it can be assumed that one of the node is the leader (primary) which determines the order of transactions and communicates it to the rest of the nodes (followers).
- Each run (3 phase commit) of the consensus protocol orders a batch (collection) of transactions.
- Nodes maintain several ledgers, each for a distinct purpose. It has a pool ledger for node membership transactions like addition of new node, suspension of a node, change of IP/port or keys of a node, a ledger for identity transactions, etc 
- Apart for the ledger, nodes maintain state (for each ledger) which is [Merkle Patricia Trie](https://github.com/ethereum/wiki/wiki/Patricia-Tree). It might maintain several other projections of the ledger. For more on storage, refer the [storage document](storage.md). 
- Clients with appropriate permissions can send write requests (transactions) to the nodes but any client can send read requests to the nodes.
- Client-to-node and node-to-node communication happens on [CurveZMQ](http://curvezmq.org/). The codebase has an abstraction called `Stack` that manages communication. It has several variants which offer different features.
- On receiving transactions nodes perform some basic validation and broadcast the request to other nodes. This is called request propagation, more details in RBFT paper. 
  Once the nodes realise that enough nodes have got the request, they consider the request ready for processing.
  The primary node initiates a new round of consensus through a 3 phase commit process at the end of which all nodes add the transaction to their ledger and corresponding storages. More details on 3 phase commit in RBFT paper.
  Different kinds of requests update different ledgers and storage layers. A detailed explanation of request handling is present [here](request_handling.md)
- During the life of the system, nodes might crash and restart, get disconnected/re-connected, new nodes might join the network. Thus nodes need a mechanism to get the transactions they have missed or never had (new node). They do this by a process called catchup.
  Here the nodes communicate their state and learn other node's states and then if the nodes realise that they are behind, they run a protocol to get the missing transactions. More on this in the [catchup document](catchup.md)
- The nodes can request various protocol-specific messages from other nodes by a `MESSAGE_REQUEST` message.
- When the primary node crashes (or becomes non functional in any way), or it misbehaves by sending bad messages or it slows down then the follower nodes initiate a protocol to change the leader, this protocol is called view change. 
  View change involves selecting a new leader, which is done in a round robin fashion and communication (and catchup if needed) of state so before the new leader starts, every node has the same state.
- The consensus round (3 phase commit) has some differences with the RBFT paper:
    1. RBFT describes a distinct 3 phase commit for each request, but in plenum, a 3 phase commit happens on batch of requests; this makes it more efficient since 3 phase commit involves n-squared communication (n being the number of nodes).  
    1. PRE-PREPARE and PREPARE contain merkle tree roots and state trie roots which are used to confirm that each node on executing the batch of requests has the same ledger and state.
    1. PRE-PREPARE contains a timestamp for the batch; the follower nodes check validate the timestamp and if valid, acknowledge with a PREPARE. The timestamp is stored in the ledger for each of the transaction.
    1. The 3 phase commit also includes a signature aggregation protocol where all nodes submit their signature on the state trie root and those signatures are aggregated and stored. Later when a client needs to query the state, the client is given a poof over the state value and the signed (aggregated signature) root.
       Thus the client does not have to rely on response from multiple nodes. The signature scheme used is [BLS](https://en.wikipedia.org/wiki/Boneh%E2%80%93Lynn%E2%80%93Shacham)
     