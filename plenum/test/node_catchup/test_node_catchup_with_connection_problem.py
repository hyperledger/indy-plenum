import sys
import types

import pytest
from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.constants import LEDGER_STATUS, CONSISTENCY_PROOF
from plenum.common.ledger_manager import LedgerManager
from plenum.common.messages.node_messages import LedgerStatus
from plenum.common.txn_util import get_req_id, get_from, get_txn_time, \
    get_payload_data
from plenum.server.replica import Replica
from plenum.test.delayers import lsDelay, msg_rep_delay, cpDelay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import checkNodesConnected, TestNode
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually
from stp_core.types import HA

call_count = 0


@pytest.fixture(scope='function', params=[1, 2, 3, 4])
def lost_count(request):
    return request.param


def test_catchup_with_lost_ledger_status(txnPoolNodeSet,
                           looper,
                           sdk_pool_handle,
                           sdk_wallet_steward,
                           tconf,
                           tdir,
                           allPluginsPath,
                           monkeypatch,
                           lost_count
                           ):
    node_to_disconnect = txnPoolNodeSet[-1]

    def ledger_status_count(status, frm):
        global call_count
        call_count += 1
        if call_count >= lost_count:
            # unpatch processLedgerStatus after lost_count calls
            monkeypatch.undo()
            call_count = 0

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)

    # restart node
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node_to_disconnect)
    looper.removeProdable(name=node_to_disconnect.name)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward,
                              2)

    nodeHa, nodeCHa = HA(*node_to_disconnect.nodestack.ha), HA(
        *node_to_disconnect.clientstack.ha)
    config_helper = PNodeConfigHelper(node_to_disconnect.name, tconf,
                                      chroot=tdir)
    node_to_disconnect = TestNode(node_to_disconnect.name,
                                  config_helper=config_helper,
                                  config=tconf,
                                  ha=nodeHa, cliha=nodeCHa,
                                  pluginPaths=allPluginsPath)
    # patch processLedgerStatus
    monkeypatch.setattr(node_to_disconnect.ledgerManager, 'processLedgerStatus',
                        ledger_status_count)

    # add node_to_disconnect to pool
    looper.add(node_to_disconnect)
    txnPoolNodeSet[-1] = node_to_disconnect
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, node_to_disconnect, *txnPoolNodeSet)


def test_catchup_with_lost_consistency_proof(txnPoolNodeSet,
                                             looper,
                                             sdk_pool_handle,
                                             sdk_wallet_steward,
                                             tconf,
                                             tdir,
                                             allPluginsPath
                                             ):
    def ledger_status_count(node):
        # assert node.ledgerManager.spylog.count(LedgerManager.processLedgerStatus) >= 2
        # assert node.master_replica.spylog.count(node.master_replica.processPrePrepare) >= 1
        assert len(node.nodeIbStasher.delayeds) >= 1
        node.nodeIbStasher.reset_delays_and_drop_delayeds()

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)
    node_to_disconnect = txnPoolNodeSet[-1]

    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node_to_disconnect)
    looper.removeProdable(name=node_to_disconnect.name)
    sdk_replies = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                            sdk_pool_handle, sdk_wallet_steward,
                                            2)

    nodeHa, nodeCHa = HA(*node_to_disconnect.nodestack.ha), HA(
        *node_to_disconnect.clientstack.ha)
    config_helper = PNodeConfigHelper(node_to_disconnect.name, tconf,
                                      chroot=tdir)
    node_to_disconnect = TestNode(node_to_disconnect.name,
                                  config_helper=config_helper,
                                  config=tconf,
                                  ha=nodeHa, cliha=nodeCHa,
                                  pluginPaths=allPluginsPath)
    node_to_disconnect.nodeIbStasher.delay(cpDelay(delay=sys.maxsize))
    looper.add(node_to_disconnect)
    txnPoolNodeSet[-1] = node_to_disconnect
    looper.run(checkNodesConnected(txnPoolNodeSet))
    looper.run(eventually(ledger_status_count, node_to_disconnect))
    waitNodeDataEquality(looper, node_to_disconnect, *txnPoolNodeSet)
