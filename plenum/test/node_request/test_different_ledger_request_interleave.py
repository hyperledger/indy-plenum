from plenum.test.helper import vdr_send_random_and_check, vdr_send_random_requests, \
    vdr_eval_timeout, vdr_get_and_check_replies
from plenum.test.node_request.helper import vdr_ensure_pool_functional
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import vdr_add_new_nym, \
    prepare_new_node_data, vdr_prepare_node_request, vdr_sign_and_send_prepared_request
from plenum.test.test_node import checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change

from plenum.test.conftest import tdirWithPoolTxns
from plenum.test.pool_transactions.conftest import sdk_node_theta_added
from plenum.test.primary_selection.conftest import sdk_one_node_added
from plenum.test.batching_3pc.conftest import tconf


def test_different_ledger_request_interleave(tconf, looper, txnPoolNodeSet,
                                             tdir,
                                             tdirWithPoolTxns,
                                             allPluginsPath,
                                             vdr_pool_handle, vdr_wallet_client,
                                             vdr_wallet_steward):
    """
    Send pool and domain ledger requests such that they interleave, and do
    view change in between and verify the pool is functional
    """
    new_node = sdk_one_node_added
    vdr_send_random_and_check(looper, txnPoolNodeSet, vdr_pool_handle,
                              vdr_wallet_client, 2)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # Send domain ledger requests but don't wait for replies
    requests = vdr_send_random_requests(looper, vdr_pool_handle,
                                        vdr_wallet_client, 2)

    # Add another node by sending pool ledger request
    _, new_theta = sdk_node_theta_added(looper,
                                        txnPoolNodeSet,
                                        tdir,
                                        tconf,
                                        vdr_pool_handle,
                                        vdr_wallet_steward,
                                        allPluginsPath,
                                        name='new_theta')

    # Send more domain ledger requests but don't wait for replies
    requests.extend(vdr_send_random_requests(looper, vdr_pool_handle,
                                             vdr_wallet_client, 3))

    # Do view change without waiting for replies
    ensure_view_change(looper, nodes=txnPoolNodeSet)
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)

    # Make sure all requests are completed
    total_timeout = vdr_eval_timeout(len(requests), len(txnPoolNodeSet))
    vdr_get_and_check_replies(looper, requests, timeout=total_timeout)
    vdr_ensure_pool_functional(looper, txnPoolNodeSet,
                               vdr_wallet_client, vdr_pool_handle)
    new_steward_wallet, steward_did = vdr_add_new_nym(looper,
                                                      vdr_pool_handle,
                                                      vdr_wallet_steward,
                                                      'another_ste',
                                                      role='STEWARD')

    # Send another pool ledger request (NODE) but don't wait for completion of
    # request
    next_node_name = 'next_node'

    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, next_node_name)
    node_req = looper.loop.run_until_complete(
        vdr_prepare_node_request(steward_did,
                             new_node_name=next_node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             key_proof=key_proof))

    sdk_wallet = (new_steward_wallet, steward_did)
    request_couple = vdr_sign_and_send_prepared_request(looper, sdk_wallet,
                                                        vdr_pool_handle,
                                                        node_req)

    # Send more domain ledger requests but don't wait for replies
    request_couples = [request_couple, *
    vdr_send_random_requests(looper, vdr_pool_handle,
                             vdr_wallet_client, 5)]

    # Make sure all requests are completed
    total_timeout = vdr_eval_timeout(len(request_couples), len(txnPoolNodeSet))
    vdr_get_and_check_replies(looper, request_couples, timeout=total_timeout)

    # Make sure pool is functional
    vdr_ensure_pool_functional(looper, txnPoolNodeSet,
                               vdr_wallet_client, vdr_pool_handle)
