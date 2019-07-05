import types

import pytest

from plenum.common.constants import TRUSTEE_STRING
from plenum.common.exceptions import PoolLedgerTimeoutException, RequestNackedException, RequestRejectedException
from plenum.test.delayers import cDelay, req_delay
from plenum.test.helper import sdk_send_random_and_check, sdk_send_random_request, sdk_get_and_check_replies, \
    sdk_signed_random_requests, sdk_send_signed_requests, sdk_sign_and_submit_req, sdk_sign_and_submit_op, assertExp
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_add_new_nym
from plenum.test.stasher import delay_rules
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

logger = getlogger()


def test_send_client_msgs_with_delay_reqs(looper,
                                          txnPoolNodeSet,
                                          sdk_pool_handle,
                                          sdk_wallet_steward):
    slow_nodes = [n.nodeIbStasher for n in txnPoolNodeSet[-2:]]
    new_wallet = sdk_add_new_nym(looper,
                                 sdk_pool_handle,
                                 sdk_wallet_steward)
    start_master_last_ordered_3PC = txnPoolNodeSet[0].master_last_ordered_3PC[1]

    print(start_master_last_ordered_3PC)
    with delay_rules(slow_nodes, req_delay()):
        resp_task = sdk_add_new_nym(looper,
                                    sdk_pool_handle,
                                    new_wallet,
                                    role=TRUSTEE_STRING,
                                    no_wait=True)
        looper.run(
            eventually(
                lambda node: assertExp(node.master_last_ordered_3PC[1] ==
                                       start_master_last_ordered_3PC + 1), txnPoolNodeSet[0]))
        ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
        print(txnPoolNodeSet[-1].master_last_ordered_3PC)

    with pytest.raises(RequestRejectedException, match="Only Steward is allowed to do these transactions"):
        _, resp = sdk_get_and_check_replies(looper, [resp_task])[0]

def test_unit_send_client_msgs_with_delay_reqs(looper,
                                          txnPoolNodeSet,
                                          sdk_pool_handle,
                                          sdk_wallet_steward):
    txnPoolNodeSet[0].
    new_wallet = sdk_add_new_nym(looper,
                                 sdk_pool_handle,
                                 sdk_wallet_steward)
    start_master_last_ordered_3PC = txnPoolNodeSet[0].master_last_ordered_3PC[1]

    print(start_master_last_ordered_3PC)
    with delay_rules(slow_nodes, req_delay()):
        resp_task = sdk_add_new_nym(looper,
                                    sdk_pool_handle,
                                    new_wallet,
                                    role=TRUSTEE_STRING,
                                    no_wait=True)
        looper.run(
            eventually(
                lambda node: assertExp(node.master_last_ordered_3PC[1] ==
                                       start_master_last_ordered_3PC + 1), txnPoolNodeSet[0]))
        ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
        print(txnPoolNodeSet[-1].master_last_ordered_3PC)

    with pytest.raises(RequestRejectedException, match="Only Steward is allowed to do these transactions"):
        _, resp = sdk_get_and_check_replies(looper, [resp_task])[0]
