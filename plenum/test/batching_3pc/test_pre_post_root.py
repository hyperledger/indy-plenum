from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_node import getNonPrimaryReplicas


def test_check_batch_pre_post_roots(tconf, looper, txnPoolNodeSet, client,
                                    wallet1):
    """
    For Pre-Prepare and Prepare, check each post root is pre root of next message.
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client, 5*tconf.Max3PCBatchSize)
    nprs = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    for r in nprs:
        last = None
        args = {(a['prepare'].viewNo, a['prepare'].ppSeqNo): a['prepare']
                for a in getAllArgs(r, r.processPrepare.__name__)}

        for key, pp in r.prePrepares.items():
            if last is None:
                last = (pp.post_state_root, pp.post_txn_root)
            else:
                assert last == (pp.pre_state_root, pp.pre_txn_root)
                p = args[key]
                assert last == (p.pre_state_root, p.pre_txn_root)
                last = (pp.post_state_root, pp.post_txn_root)

