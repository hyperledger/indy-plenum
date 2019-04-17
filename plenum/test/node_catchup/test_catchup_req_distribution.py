from random import randint
from typing import Dict, Tuple

import math
import pytest

from plenum.common.messages.node_messages import CatchupReq


def __gen_catchup_reqs(ledger_id, start, end, node_count):
    batch_length = math.ceil((end - start) / node_count)
    reqs = []
    s = start + 1
    e = min(s + batch_length - 1, end)
    for i in range(node_count):
        req = CatchupReq(ledger_id, s, e, end)
        reqs.append(req)
        s = e + 1
        e = min(s + batch_length - 1, end)
        if s > end:
            break
    return reqs


def build_catchup_reqs(ledger_id: int, start_seq_no: int, end_seq_no: int,
                       pool_txns: Dict[str, int]) -> Dict[str, CatchupReq]:
    txns_to_catchup = end_seq_no - start_seq_no
    if txns_to_catchup == 0:
        return {}

    reqs = __gen_catchup_reqs(ledger_id, start_seq_no, end_seq_no, len(pool_txns))
    return {node_id: req for node_id, req in zip(pool_txns.keys(), reqs)}


def test_catchup_req_distribution_invariants():
    for _ in range(1000):
        # Setup
        num_nodes = randint(4, 10)
        pool_txns = {str(idx + 1): randint(0, 1000) for idx in range(num_nodes)}
        node_id = str(randint(1, num_nodes))
        catchup_till = randint(pool_txns[node_id], max(pool_txns.values()))
        catchup_from = pool_txns[node_id]
        del pool_txns[node_id]
        catchup_reqs = build_catchup_reqs(1, catchup_from, catchup_till, pool_txns)
        context = "from {} to {}\npool {}\ncatchup reqs {}".\
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
            assert len(txns_requested) == catchup_till - catchup_from, context
            for a, b in zip(txns_requested, txns_requested[1:]):
                assert b - a == 1, context
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
