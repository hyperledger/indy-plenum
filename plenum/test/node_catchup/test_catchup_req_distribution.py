from random import randint, shuffle
from typing import Dict, List, Tuple, Optional

from plenum.common.messages.node_messages import CatchupReq


def build_catchup_reqs(ledger_id: int, start_seq_no: int, end_seq_no: int,
                       pool_txns: Dict[str, int]) -> Dict[str, CatchupReq]:
    # Utility
    def find_node_idx(nodes_txns: List[Tuple[str, int]], max_seq_no: int) -> int:
        for i, (_, txns) in enumerate(nodes_txns):
            if txns >= max_seq_no:
                return i

    def find_next_best_node_idx(nodes_txns: List[Tuple[str, int]], exclude_idx) -> int:
        idx_txns = ((idx, txns)
                    for idx, (_, txns) in enumerate(nodes_txns)
                    if idx != exclude_idx)
        return max(idx_txns, key=lambda v: v[1])[0]

    # Gather all nodes that have transactions we potentially need.
    # Register nodes having more than needed transactions as having only
    # needed transactions to reduce ability to manipulate distribution
    # of catchup requests by malicious nodes
    nodes_txns = [(node_id, min(txns, end_seq_no))
                  for node_id, txns in pool_txns.items()
                  if txns > start_seq_no]

    # Shuffle nodes so that catchup requests will be sent randomly
    shuffle(nodes_txns)

    reqs = {}
    pos = end_seq_no
    while len(nodes_txns) > 0:
        txns_to_catchup = (pos - start_seq_no) // len(nodes_txns)

        node_index = find_node_idx(nodes_txns, pos)
        if len(nodes_txns) > 1:
            next_node_index = find_next_best_node_idx(nodes_txns, node_index)
            next_node_txns = nodes_txns[next_node_index][1]
            if pos - txns_to_catchup > next_node_txns:
                txns_to_catchup = pos - next_node_txns

        if txns_to_catchup > 0:
            node_id = nodes_txns[node_index][0]
            reqs[node_id] = CatchupReq(ledgerId=ledger_id,
                                       seqNoStart=pos - txns_to_catchup + 1,
                                       seqNoEnd=pos,
                                       catchupTill=end_seq_no)
            pos -= txns_to_catchup

        del nodes_txns[node_index]

    return reqs


def test_catchup_req_distribution_invariants():
    for _ in range(2000):
        # Setup
        num_nodes = randint(4, 10)
        pool_txns = {str(idx + 1): randint(0, 1000) for idx in range(num_nodes)}
        node_id = str(randint(1, num_nodes))
        catchup_till = randint(pool_txns[node_id], max(pool_txns.values()))
        catchup_from = pool_txns[node_id]
        del pool_txns[node_id]
        catchup_reqs = build_catchup_reqs(1, catchup_from, catchup_till, pool_txns)
        context = "from {} to {}\npool {}\ncatchup reqs {}". \
            format(catchup_from, catchup_till, pool_txns, catchup_reqs)

        # Gather all requested transactions
        txns_requested = []
        for req in catchup_reqs.values():
            for seq_no in range(req.seqNoStart, req.seqNoEnd + 1):
                txns_requested.append(seq_no)
        txns_requested.sort()

        # Check requested transactions invariants
        if catchup_till - catchup_from > 0:
            assert len(catchup_reqs) > 0, context
            assert txns_requested[0] == catchup_from + 1, context
            assert txns_requested[-1] == catchup_till, context
            for a, b in zip(txns_requested, txns_requested[1:]):
                assert b - a == 1, context
            assert len(txns_requested) == catchup_till - catchup_from, context
        else:
            assert len(catchup_reqs) == 0, context
            assert len(txns_requested) == 0, context

        # Check individual requests invariants
        for node_id, req in catchup_reqs.items():
            req_context = "node {}, req {}\n{}".format(node_id, req, context)

            assert node_id in pool_txns.keys(), req_context
            assert req.ledgerId == 1, req_context
            assert req.catchupTill == catchup_till, req_context
            assert req.seqNoStart <= req.seqNoEnd, req_context
            assert req.seqNoEnd <= pool_txns[node_id], req_context
