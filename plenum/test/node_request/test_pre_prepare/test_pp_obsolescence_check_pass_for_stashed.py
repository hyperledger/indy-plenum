import pytest

from plenum.test import waits
from plenum.common.constants import LEDGER_STATUS, AUDIT_LEDGER_ID
from plenum.common.messages.node_messages import MessageRep, MessageReq, CatchupReq
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import DEFAULT_DELAY
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_request.test_timestamp.helper import get_timestamp_suspicion_count
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules

nodeCount = 4

# should be big enough to pass PP during normal ordering flow
PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS = waits.expectedPrepareTime(nodeCount)


@pytest.fixture(scope="module")
def tconf(tconf):
    old_value = tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS
    tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS = PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS
    yield tconf
    tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS = old_value


def delay_audit_ledger_catchup():
    def delay(msg):
        msg = msg[0]
        if isinstance(msg, MessageRep) and \
                msg.msg_type == LEDGER_STATUS and \
                msg.params.get('ledgerId') == AUDIT_LEDGER_ID:
            return DEFAULT_DELAY

    return delay


def test_stashed_pp_pass_obsolescence_check(tdir, tconf,
                                     looper,
                                     txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client):
    lagging_node = txnPoolNodeSet[-1]

    def lagging_node_state() -> NodeLeecherService.State:
        return lagging_node.ledgerManager._node_leecher._state

    # TODO INDY-2047: fills domain ledger with some requests
    # as a workaround for the issue
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    # Prevent lagging node from catching up domain ledger (and finishing catchup)
    with delay_rules(lagging_node.nodeIbStasher, delay_audit_ledger_catchup()):
        # Start catchup on lagging node
        lagging_node.ledgerManager.start_catchup()
        assert lagging_node_state() == NodeLeecherService.State.SyncingAudit

        # Order request on all nodes except lagging one where they goes to stashed state
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 1)

        # lagging node is still syncing Audit ledger
        assert lagging_node_state() == NodeLeecherService.State.SyncingAudit

        # delay catchup end to exceed PP ACCEPTABLE_DEVIATION_PREPREPARE_SECS
        looper.runFor(PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS + 1)

    # Now catchup should end and lagging node starts processing stashed PPs
    # and resumes ordering

    # ensure that all nodes will have same data after that
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # ensure that no suspicions about obsolete PP have been raised
    assert get_timestamp_suspicion_count(lagging_node) == 0
