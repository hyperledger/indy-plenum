import pytest
import zmq

from plenum.common.constants import TRUSTEE_STRING
from plenum.common.exceptions import RequestRejectedException
from plenum.test.helper import sdk_get_and_check_replies, assertExp, sdk_send_random_requests
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_add_new_nym, sdk_add_new_steward_and_node, sdk_pool_refresh
from plenum.test.test_node import checkNodesConnected
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

logger = getlogger()


def test_resending_pending_client_msgs(looper,
                                       txnPoolNodeSet,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       sdk_wallet_steward,
                                       tdir, tconf, allPluginsPath,
                                       monkeypatch):
    problem_node = txnPoolNodeSet[1]
    
    def fail_send_multipart(msg_parts, flags=0, copy=True, track=False, **kwargs):
        raise zmq.ZMQError(113, "")

    # Switch off replies for client from Beta, Gamma, Delta
    for node in txnPoolNodeSet[2:]:
        monkeypatch.setattr(node.clientstack.listener, 'send_multipart',
                            lambda msg_parts, flags=0, copy=True, track=False, **kwargs: None)
    monkeypatch.setattr(problem_node.clientstack.listener, 'send_multipart',
                        fail_send_multipart)

    start_master_last_ordered_3pc = txnPoolNodeSet[0].master_last_ordered_3PC[1]
    # Send the first request. Nodes should reject it.
    resp_task = sdk_add_new_nym(looper,
                                sdk_pool_handle,
                                sdk_wallet_client,
                                role=TRUSTEE_STRING,
                                no_wait=True)
    looper.run(
        eventually(
            lambda node: assertExp(node.master_last_ordered_3PC[1] ==
                                   start_master_last_ordered_3pc + 1), txnPoolNodeSet[0]))
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    monkeypatch.delattr(problem_node.clientstack.listener, 'send_multipart', raising=True)

    # Send the second request.
    sdk_reqs = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)

    # Waiting reject for the first request, which will sent with a reply for the second request.
    with pytest.raises(RequestRejectedException, match="Only Steward is allowed to do these transactions"):
        _, resp = sdk_get_and_check_replies(looper, [resp_task])[0]

    # Waiting a rely for the second request
    sdk_get_and_check_replies(looper, sdk_reqs)
    monkeypatch.undo()
