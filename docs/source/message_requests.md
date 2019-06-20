# Message Requests

In BFT protocols you can lose no more than a certain number of messages. In Plenum it may happen in the following valid cases:
- A node received more number of messages than maximum size of a ZMQ queue.
- A node discarded messages because it was unable to process it.
- A node disconnected for a short time.

In plenum there are classes `MessageReq` for message requests and `MessageRep` for replies. `MessageReqProcessor` manages receiving, sending and processing request and reply messages.

###Three-phase commit messages

If a node lost a lot of messages for three-phase commit, ordering can stop for this node until a catch-up is triggered by a checkpoint. The problem is successfully solved via message request mechanism.
A node requests lost messages from other nodes in follow cases:
- All PrePrepares need to be applied sequentially, without any gaps. If a node receives a **PrePrepare** where previous PrePrepare(s) are not received yet or lost, it **asks PrePrepares, Prepares, Commits** from the node last pp_seq_no to received PrePrepare (pp_seq_no - 1). If the number of messages to be requested is more than CHK_FREQ, that is the size of Checkpoint, then nothing is requested since the Node is falling behind and is going to start a catch-up in any case because of a quorum of stashed stable checkpoints.
- A node receives a **Prepare** for which it doesn't have the corresponding PrePrepare -> **ask PrePrepares** for pp_seq_no from received Prepare
- A node receives a **PrePrepare for a not finalized request** (that is request for which the Node doesn't have a quorum of Propagates) -> ask **Propagates** for not finalize requests from PrePrepare

For system security, there are the following message request rules:
- Lost Pre-prepares are requested only from the primary node
- Lost Commits, Prepares and Propagates are requested from all nodes

###Catchup messages

Lost messages may delay catchup for a long time. The node message requests mechanism is used in follow cases:
- A node requests a LedgerStatus in a start of catchup process to compare with self ledger.
- With the first message request for a LedgerStatus the node is scheduling a new message request in config.LedgerStatusTimeout for case the answer to the first request is not received.
- A node requests a ConsistencyProof with median rate of ledger size on other nodes.
- With the first message request for a ConsistencyProof the node is scheduling a new message request in config.ConsistencyProofTimeout for case the answer to the first request is not received.

If the node has the quorum(n - f - 1) of LedgerStatuses or the quorum(f + 1) of ConsistencyProofs, scheduled request will be canceled.