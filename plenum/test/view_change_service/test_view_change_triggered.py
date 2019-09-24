from functools import partial

import pytest

from plenum.common.messages.internal_messages import NeedViewChange
from plenum.common.util import getMaxFailures
from plenum.server.consensus.ordering_service_msg_validator import OrderingServiceMsgValidator
from plenum.server.consensus.primary_selector import RoundRobinPrimariesSelector
from plenum.test.delayers import cDelay
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone, getNonPrimaryReplicas

from stp_core.common.log import Logger
Logger().enableStdLogging()
Logger().setLogLevel(0)


REQ_COUNT = 10


@pytest.fixture(scope="module", params=['once', 'twice'])
def vc_counts(request):
    return request.param


@pytest.fixture(scope="module", params=[True, False])
def slow_node_is_next_primary(request):
    return request.param


@pytest.fixture(scope="module")
def tconf(tconf):
    old_new_view_timeout = tconf.NEW_VIEW_TIMEOUT
    old_batch_size = tconf.Max3PCBatchSize
    tconf.NEW_VIEW_TIMEOUT = 5
    tconf.Max3PCBatchSize = 1
    yield tconf
    tconf.Max3PCBatchSize = old_batch_size
    tconf.NEW_VIEW_TIMEOUT = old_new_view_timeout


@pytest.fixture(scope="module")
def txnPoolNodeSet(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        for replica in node.replicas.values():
            replica._ordering_service._validator = OrderingServiceMsgValidator(replica._consensus_data)
        node._view_changer.start_view_change = partial(trigger_view_change_on_node, node)
    yield txnPoolNodeSet


def trigger_view_change_on_node(node, proposed_view_no):
    for r in node.replicas.values():
        r.internal_bus.send(NeedViewChange(proposed_view_no))
        if r.isMaster:
            assert r._consensus_data.waiting_for_new_view


def trigger_view_change(txnPoolNodeSet, proposed_view_no):
    for node in txnPoolNodeSet:
        trigger_view_change_on_node(node, proposed_view_no)


def get_next_primary_name(txnPoolNodeSet, expected_view_no):
    selector = RoundRobinPrimariesSelector()
    inst_count = len(txnPoolNodeSet[0].replicas)
    next_p_name = selector.select_primaries(expected_view_no, inst_count, txnPoolNodeSet[0].poolManager.node_names_ordered_by_rank())[0]
    return next_p_name


def test_view_change_triggered(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    trigger_view_change(txnPoolNodeSet, current_view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


def test_view_change_triggered_after_ordering(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, REQ_COUNT)
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    trigger_view_change(txnPoolNodeSet, current_view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


# @pytest.mark.skip(reason="not working now")
def test_view_change_with_next_primary_stopped(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    next_primary = get_next_primary_name(txnPoolNodeSet, old_view_no + 1)
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, next_primary)
    trigger_view_change(txnPoolNodeSet, old_view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    assert current_view_no == old_view_no + 2


def test_delay_commits_for_one_node(looper,
                                    txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_client,
                                    slow_node_is_next_primary,
                                    vc_counts):
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    exepted_view_no = current_view_no + 1 if vc_counts == 'once' else current_view_no + 2
    next_primary = get_next_primary_name(txnPoolNodeSet, exepted_view_no)
    pretenders = [r.node for r in getNonPrimaryReplicas(txnPoolNodeSet) if not r.isPrimary]
    if slow_node_is_next_primary:
        delayed_node = [n for n in pretenders if n.name == next_primary][0]
    else:
        delayed_node = [n for n in pretenders if n.name != next_primary][0]

    with delay_rules_without_processing(delayed_node.nodeIbStasher, cDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2)

        trigger_view_change(txnPoolNodeSet, current_view_no + 1)
        if vc_counts == 'twice':
            trigger_view_change(txnPoolNodeSet, current_view_no + 2)

    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
