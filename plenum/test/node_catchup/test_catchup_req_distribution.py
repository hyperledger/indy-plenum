from random import randint
from typing import Dict

from plenum.common.messages.node_messages import CatchupRep


def build_catchup_reqs(ledger_id: int, start_seq_no: int, end_seq_no: int,
                       pool_txns: Dict[str, int]) -> Dict[str, CatchupRep]:
    return {}


def check_catchup_req_distribution_invariants(catchup_from: int, catchup_till: int,
                                              pool_txns: Dict[str, int],
                                              catchup_reqs: Dict[str, CatchupRep]):
    # Gather all requested transactions
    txns_requested = []
    for req in catchup_reqs:
        for seq_no in range(req.seqNoStart, req.seqNoEnd + 1):
            txns_requested.append(seq_no)
    txns_requested.sort()

    # Check requested transactions invariants
    if catchup_till - catchup_from > 0:
        assert len(catchup_reqs) > 0
        assert txns_requested[0] == catchup_from + 1
        assert txns_requested[-1] == catchup_till
        assert len(txns_requested) == catchup_till - catchup_from
        for a, b in zip(txns_requested, txns_requested[1:]):
            assert b - a == 1
    else:
        assert len(catchup_reqs) == 0
        assert len(txns_requested) == 0

    # Check individual requests invariants
    for node_id, req in catchup_reqs:
        assert node_id in pool_txns.keys()
        assert req.ledgerId == 1
        assert req.catchupTill == catchup_till
        assert req.seqNoStart <= req.seqNoEnd
        assert req.seqNoEnd <= pool_txns[node_id]


def test_catchup_req_distribution_invariants():
    for _ in range(1000):
        num_nodes = randint(4, 25)
        pool_txns = {str(idx + 1): randint(0, 1000) for idx in range(num_nodes)}
        node_id = str(randint(1, num_nodes))
        catchup_till = randint(pool_txns[node_id], max(pool_txns.values()))
        catchup_from = pool_txns[node_id]
        del pool_txns[node_id]

        catchup_reqs = build_catchup_reqs(1, catchup_from, catchup_till, pool_txns)
        check_catchup_req_distribution_invariants(catchup_from, catchup_till, pool_txns, catchup_reqs)
