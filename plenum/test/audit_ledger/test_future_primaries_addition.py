import copy
import time

from plenum.common.request import Request
from plenum.test.delayers import cDelay

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import randomString

from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.test.stasher import delay_rules

from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import add_new_node

from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.pool_transactions.helper import demote_node

nodeCount = 6

old_commit = None


def test_future_primaries_replicas_increase(looper, txnPoolNodeSet, sdk_pool_handle,
                                            sdk_wallet_stewards, tdir, tconf, allPluginsPath):
    # Don't delete NodeStates, so we could check them.
    global old_commit
    old_commit = txnPoolNodeSet[0].write_manager.future_primary_handler.commit_batch
    for node in txnPoolNodeSet:
        node.write_manager.future_primary_handler.commit_batch = lambda three_pc_batch, prev_handler_result=None: 0

    initial_primaries = copy.copy(txnPoolNodeSet[0].primaries)
    last_ordered = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    starting_view_number = checkViewNoForNodes(txnPoolNodeSet)

    # Increase replicas count
    add_new_node(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], tdir, tconf, allPluginsPath)

    new_view_no = checkViewNoForNodes(txnPoolNodeSet)
    assert new_view_no == starting_view_number + 1
    # "seq_no + 2" because 1 domain and 1 pool txn.

    node = txnPoolNodeSet[0]
    with delay_rules(node.nodeIbStasher, cDelay()):
        req = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_stewards[0], 1)[0][0]
        req = Request(**req)
        three_pc_batch = ThreePcBatch(DOMAIN_LEDGER_ID, 0, 0, 1, time.time(),
                                      randomString(),
                                      randomString(),
                                      ['a', 'b', 'c'], [req.digest], pp_digest='')
        primaries = node.write_manager.future_primary_handler.post_batch_applied(three_pc_batch)
        assert len(primaries) == len(initial_primaries) + 1
        assert len(primaries) == len(node.primaries)


def test_future_primaries_replicas_decrease(looper, txnPoolNodeSet, sdk_pool_handle,
                                            sdk_wallet_stewards, tdir, tconf, allPluginsPath):
    assert len(txnPoolNodeSet) == 7

    initial_primaries = copy.copy(txnPoolNodeSet[0].primaries)
    last_ordered = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    starting_view_number = checkViewNoForNodes(txnPoolNodeSet)

    # Decrease replicas count
    demote_node(looper, sdk_wallet_stewards[-1], sdk_pool_handle, txnPoolNodeSet[-2])
    txnPoolNodeSet.remove(txnPoolNodeSet[-2])
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    new_view_no = checkViewNoForNodes(txnPoolNodeSet)
    assert new_view_no == starting_view_number + 1
    node = txnPoolNodeSet[0]
    with delay_rules(node.nodeIbStasher, cDelay()):
        req = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_stewards[0], 1)[0][0]
        req = Request(**req)
        three_pc_batch = ThreePcBatch(DOMAIN_LEDGER_ID, 0, 0, 1, time.time(),
                                      randomString(),
                                      randomString(),
                                      ['a', 'b', 'c'], [req.digest], pp_digest='')
        primaries = node.write_manager.future_primary_handler.post_batch_applied(three_pc_batch)
        assert len(primaries) + 1 == len(initial_primaries)
        assert len(primaries) == len(txnPoolNodeSet[0].primaries)

    for node in txnPoolNodeSet:
        node.write_manager.future_primary_handler.commit_batch = old_commit
