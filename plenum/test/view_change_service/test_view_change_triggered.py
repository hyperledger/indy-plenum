from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from functools import partial

import pytest
from plenum.test.delayers import cDelay
from plenum.common.messages.internal_messages import NeedViewChange
from plenum.server.consensus.ordering_service_msg_validator import OrderingServiceMsgValidator
from plenum.server.consensus.primary_selector import RoundRobinPrimariesSelector
from plenum.test.delayers import cDelay
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check, assertExp
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone, getNonPrimaryReplicas

from stp_core.common.log import Logger
from stp_core.loop.eventually import eventually


from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change_service.helper import trigger_view_change, \
    get_next_primary_name


REQ_COUNT = 10


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


def test_view_change_with_next_primary_stopped(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    next_primary = get_next_primary_name(txnPoolNodeSet, old_view_no + 1)
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, next_primary)
    remaining_nodes = [node for node in txnPoolNodeSet if node.name != next_primary]
    trigger_view_change(remaining_nodes, old_view_no + 1)
    ensureElectionsDone(looper, remaining_nodes, instances_list=range(2), customTimeout=15)
    sdk_ensure_pool_functional(looper, remaining_nodes, sdk_wallet_client, sdk_pool_handle)
    current_view_no = checkViewNoForNodes(remaining_nodes)
    assert current_view_no == old_view_no + 2
