import sys

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import PrePrepare
from plenum.common.request import Request
from plenum.common.util import get_utc_epoch
from plenum.server.node import Node
from plenum.server.replica import Replica
from plenum.test import waits
from plenum.test.bls.helper import create_pre_prepare_no_bls, init_discarded
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.delayers import cDelay
from plenum.test.node_catchup.test_config_ledger import start_stopped_node
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.replica.helper import check_replica_removed
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_requests, sdk_get_replies, sdk_send_random_and_check, waitForViewChange
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected, \
    get_master_primary_node, get_last_master_non_primary_node

from plenum.test.checkpoints.conftest import chkFreqPatched

logger = getlogger()
CHK_FREQ = 1

def test_replica_received_preprepare_with_ordered_request(looper,
                                                          txnPoolNodeSet,
                                                          sdk_pool_handle,
                                                          sdk_wallet_steward,
                                                          chkFreqPatched):
    requests = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_steward, 1)
    replica = txnPoolNodeSet[1].master_replica
    pp = replica.spylog.getLastParams(Replica.processPrePrepare)
    replica.processPrePrepare(pp["pre_prepare"], pp["sender"])
    assert 0 == replica.node.spylog.count(Node.request_propagates)
    assert (pp, pp["sender"], set(pp.reqIdr)) not in replica.prePreparesPendingFinReqs
    requests = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_steward, 1)
