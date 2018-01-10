import pytest
from plenum.common.util import randomString
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.view_change.helper import ensure_several_view_change
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test.test_node import checkNodesConnected
from plenum.test.node_catchup.helper import ensureClientConnectedToNodesAndPoolLedgerSame, waitNodeDataEquality


def _get_ppseqno(nodes):
    res = set()
    for node in nodes:
        for repl in node.replicas:
            res.add(repl.lastPrePrepareSeqNo)
    assert(len(res) == 1)
    return min(res)


def _set_ppseqno(nodes, new_ppsn):
    for node in nodes:
        for repl in node.replicas:
            repl.lastPrePrepareSeqNo = new_ppsn
            repl.h = new_ppsn
            repl.last_ordered_3pc = (repl.viewNo, new_ppsn)


@pytest.mark.parametrize('do_view_change', [0, 1])
def test_add_node_to_pool_with_large_ppseqno_diff_views(do_view_change, looper, txnPoolNodeSet, tconf, steward1,
                                                        stewardWallet, tdir, client_tdir, allPluginsPath):
    """
    Adding a node to the pool while ppSeqNo is big caused a node to stash all the
    requests because of incorrect watermarks limits set.
    The case of view_no == 0 is special.
    The test emulates big ppSeqNo number, adds a node and checks all the pool nodes
    are functional. The test is run with several starting view_no, including 0
    """

    # TODO: for now this test will use old client api, after moving node txn to sdk it will be rewritten

    ensure_several_view_change(looper, txnPoolNodeSet, do_view_change, custom_timeout=tconf.VIEW_CHANGE_TIMEOUT)

    big_ppseqno = tconf.LOG_SIZE * 2 + 2345
    cur_ppseqno = _get_ppseqno(txnPoolNodeSet)
    assert(big_ppseqno > cur_ppseqno)

    # ensure pool is working properly
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, numReqs=3)
    assert(cur_ppseqno < _get_ppseqno(txnPoolNodeSet))

    _set_ppseqno(txnPoolNodeSet, big_ppseqno)
    cur_ppseqno = _get_ppseqno(txnPoolNodeSet)
    assert (big_ppseqno == cur_ppseqno)
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, numReqs=3)
    assert (cur_ppseqno < _get_ppseqno(txnPoolNodeSet))

    new_steward_name = "testClientSteward" + randomString(4)
    new_node_name = "TestTheta" + randomString(4)
    new_steward, new_steward_wallet, new_node =\
        addNewStewardAndNode(looper, steward1, stewardWallet, new_steward_name,
                             new_node_name, tdir, client_tdir, tconf, allPluginsPath)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1, *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, new_steward, *txnPoolNodeSet)

    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])

    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, steward1, numReqs=3)

    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
