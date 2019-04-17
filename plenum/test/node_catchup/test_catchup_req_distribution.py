from random import randint

from plenum.server.catchup.catchup_rep_service import CatchupRepService


def test_catchup_req_distribution_invariants():
    for _ in range(2000):
        # Setup
        num_nodes = randint(4, 10)
        pool_txns = {str(idx + 1): randint(0, 1000) for idx in range(num_nodes)}
        node_id = str(randint(1, num_nodes))
        start_seq_no = pool_txns[node_id] + 1
        end_seq_no = randint(start_seq_no - 1, max(pool_txns.values()))
        catchup_till = randint(end_seq_no, max(pool_txns.values()))
        del pool_txns[node_id]
        catchup_reqs = CatchupRepService._build_catchup_reqs(1,
                                                             start_seq_no,
                                                             end_seq_no,
                                                             catchup_till,
                                                             pool_txns)
        context = "from {} to {}\npool {}\ncatchup reqs {}". \
            format(start_seq_no, end_seq_no, pool_txns, catchup_reqs)

        # Gather all requested transactions
        txns_requested = []
        for req in catchup_reqs.values():
            for seq_no in range(req.seqNoStart, req.seqNoEnd + 1):
                txns_requested.append(seq_no)
        txns_requested.sort()

        # Check requested transactions invariants
        if end_seq_no - start_seq_no + 1 > 0:
            assert len(catchup_reqs) > 0, context
            assert txns_requested[0] == start_seq_no, context
            assert txns_requested[-1] == end_seq_no, context
            for a, b in zip(txns_requested, txns_requested[1:]):
                assert b - a == 1, context
            assert len(txns_requested) == end_seq_no - start_seq_no + 1, context
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
