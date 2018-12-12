# Message Requests

In BFT protocols you can lose no more than a certain count of messages. But it may happen in following valid cases:
- A node received more number of messages than maximum size of a ZMQ queue.
- A node discarded messages because it was unable to process it.
- A node disconnected for a short time.

In plenum there are classes `MessageReq` for message requests and `MessageRep` for replies. `MessageReqProcessor` manages receiving, sending and processing request and reply messages.

###Three-phase commit messages

If a node lost a lot of messages for three-phase commit, ordering can stop for ever for this node. The problem is successfully solved via message request mechanism.
A node requests lost messages from other nodes in follow cases:
- A node received not next PrePrepare -> ask PrePrepares, Prepares, Commits from the node last pp_seq_no to received PrePrepare (pp_seq_no - 1)
- A node received not next Prepare -> ask PrePrepares for pp_seq_no from received Prepare
- A node received PrePrepare for not finalized request -> ask Propagates for not finalize requests from PrePrepare

For system security, there are the following message request rules:
- Lost Pre-prepares are requested only from the primary node
- Lost Commits, Prepares and Propagates are requested from all nodes

###Catchup messages

Lost messages may delay catchup for a long time. The node message requests mechanism is used in follow cases:
- A node requests a LedgerStatus in a start of catchup process to compare with self ledger.
- With the first message request for a LedgerStatus the node is scheduling a new message request in config.LedgerStatusTimeout for case the answer to the first request will not be received. 
- A node requests a ConsistencyProof with median rate of ledger size on other nodes.
- With the first message request for a ConsistencyProof the node is scheduling a new message request in config.ConsistencyProofTimeout for case the answer to the first request will not be received.

If the node has the quorum(n - f - 1) of LedgerStatuses or the quorum(f + 1) of ConsistencyProofs, scheduled request will be canceled.