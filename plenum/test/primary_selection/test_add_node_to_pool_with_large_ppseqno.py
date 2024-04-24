import pytest

from plenum.common.messages.node_messages import Checkpoint
from plenum.common.util import randomString
from plenum.test.helper import get_pp_seq_no
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.view_change.helper import ensure_several_view_change
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.test_node import checkNodesConnected
from plenum.test.node_catchup.helper import waitNodeDataEquality


def _set_ppseqno(nodes, new_ppsn):
    for node in nodes:
        for repl in node.replicas.values():
            repl._ordering_service.lastPrePrepareSeqNo = new_ppsn
            repl._checkpointer.set_watermarks(low_watermark=new_ppsn)
            st_chk = new_ppsn - new_ppsn % 100
            repl._consensus_data.stable_checkpoint = st_chk
            repl._consensus_data.checkpoints.add(Checkpoint(0, repl.viewNo, st_chk - 100, st_chk, None))
            repl.last_ordered_3pc = (repl.viewNo, new_ppsn)


@pytest.mark.parametrize('do_view_change', [0, 1])
def test_add_node_to_pool_with_large_ppseqno_diff_views(do_view_change, looper, txnPoolNodeSet, tconf, sdk_pool_handle,
                                                        sdk_wallet_steward, tdir, allPluginsPath):
    """
    Adding a node to the pool while ppSeqNo is big caused a node to stash all the
    requests because of incorrect watermarks limits set.
    The case of view_no == 0 is special.
    The test emulates big ppSeqNo number, adds a node and checks all the pool nodes
    are functional. The test is run with several starting view_no, including 0
    """

    ensure_several_view_change(looper, txnPoolNodeSet, do_view_change, custom_timeout=tconf.NEW_VIEW_TIMEOUT)

    cur_ppseqno = get_pp_seq_no(txnPoolNodeSet)
    big_ppseqno = cur_ppseqno + tconf.LOG_SIZE * 2 + 2300
    assert (big_ppseqno > cur_ppseqno)

    # ensure pool is working properly
    sdk_ensure_pool_functional(looper, txnPoolNodeSet,
                               sdk_wallet_steward,
                               sdk_pool_handle)
    assert (cur_ppseqno < get_pp_seq_no(txnPoolNodeSet))

    _set_ppseqno(txnPoolNodeSet, big_ppseqno)
    cur_ppseqno = get_pp_seq_no(txnPoolNodeSet)
    assert (big_ppseqno == cur_ppseqno)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet,
                               sdk_wallet_steward,
                               sdk_pool_handle)

    assert (cur_ppseqno < get_pp_seq_no(txnPoolNodeSet))



    # Disable view change after adding new node as it will not be able to finish due to fake ppSeqNo set
    for n in txnPoolNodeSet:
        n.write_manager.node_reg_handler.internal_bus = None

    new_steward_name = "testClientSteward" + randomString(4)
    new_node_name = "TestTheta" + randomString(4)
    new_steward_wallet_handle, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        new_steward_name, new_node_name, tdir, tconf,
        allPluginsPath=allPluginsPath)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    sdk_ensure_pool_functional(looper, txnPoolNodeSet,
                               new_steward_wallet_handle,
                               sdk_pool_handle)

    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])

    sdk_ensure_pool_functional(looper, txnPoolNodeSet,
                               sdk_wallet_steward,
                               sdk_pool_handle)

    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
