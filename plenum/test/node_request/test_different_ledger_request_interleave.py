import json

from indy.ledger import build_node_request
from plenum.common.signer_simple import SimpleSigner

from plenum.test.node_request.helper import sdk_ensure_pool_functional

from plenum.test.helper import sdk_send_random_and_check, sdk_send_random_requests, \
    sdk_eval_timeout, sdk_get_replies, sdk_check_reply, sdk_sign_request_objects, \
    sdk_send_signed_requests, sdk_get_and_check_replies, sdk_json_to_request_object
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_add_new_steward, prepare_new_node_data
from plenum.test.test_node import checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change

from plenum.test.conftest import tdirWithPoolTxns
from plenum.test.pool_transactions.conftest import sdk_node_theta_added
from plenum.test.primary_selection.conftest import sdk_one_node_added
from plenum.test.batching_3pc.conftest import tconf


def test_different_ledger_request_interleave(tconf, looper, txnPoolNodeSet,
                                             sdk_one_node_added,
                                             tdir,
                                             tdirWithPoolTxns,
                                             allPluginsPath,
                                             sdk_pool_handle, sdk_wallet_client,
                                             sdk_wallet_steward):
    """
    Send pool and domain ledger requests such that they interleave, and do
    view change in between and verify the pool is functional
    """
    new_node = sdk_one_node_added
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 2)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # Send domain ledger requests but don't wait for replies
    requests = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 2)

    # Add another node by sending pool ledger request
    _, new_theta = sdk_node_theta_added(looper,
                                        txnPoolNodeSet,
                                        tdir,
                                        tconf,
                                        sdk_pool_handle,
                                        sdk_wallet_steward,
                                        allPluginsPath,
                                        name='new_theta')

    # Send more domain ledger requests but don't wait for replies
    requests.extend(sdk_send_random_requests(looper, sdk_pool_handle,
                                             sdk_wallet_client, 3))

    # Do view change without waiting for replies
    ensure_view_change(looper, nodes=txnPoolNodeSet)
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)

    # Make sure all requests are completed
    total_timeout = sdk_eval_timeout(len(requests), len(txnPoolNodeSet))
    sdk_replies = sdk_get_replies(looper, requests, timeout=total_timeout)
    for req_res in sdk_replies:
        sdk_check_reply(req_res)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet,
                               sdk_wallet_client, sdk_pool_handle)
    new_steward_wallet, steward_did = sdk_add_new_steward(looper,
                                                          sdk_pool_handle,
                                                          sdk_wallet_steward,
                                                          'another_ste')

    # Send another pool ledger request (NODE) but don't wait for completion of
    # request
    next_node_name = 'next_node'

    node_req = looper.loop.run_until_complete(
        build_request(tconf, tdir, next_node_name, steward_did))
    sdk_wallet = (new_steward_wallet, steward_did)
    signed_reqs = sdk_sign_request_objects(looper, sdk_wallet,
                                           [sdk_json_to_request_object(
                                               json.loads(node_req))])
    request_couple = sdk_send_signed_requests(sdk_pool_handle, signed_reqs)[0]

    # Send more domain ledger requests but don't wait for replies
    request_couples = [request_couple, *
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 5)]

    # Make sure all requests are completed
    total_timeout = sdk_eval_timeout(len(request_couples), len(txnPoolNodeSet))
    sdk_get_and_check_replies(looper, request_couples)

    # Make sure pool is functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet,
                               sdk_wallet_client, sdk_pool_handle)


async def build_request(tconf, tdir, next_node_name, steward_did):
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort = \
        prepare_new_node_data(tconf, tdir, next_node_name)
    data = {
        'alias': next_node_name,
        'client_ip': clientIp,
        'client_port': clientPort,
        'node_ip': nodeIp,
        'node_port': nodePort,
        'services': ["VALIDATOR"],
        'blskey': bls_key
    }
    nodeSigner = SimpleSigner(seed=sigseed)
    destination = nodeSigner.identifier
    node_request = await build_node_request(steward_did, destination, json.dumps(data))
    return node_request
