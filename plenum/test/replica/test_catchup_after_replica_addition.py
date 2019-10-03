import pytest
from plenum.common.constants import STEWARD_STRING, VALIDATOR
from pytest import fixture

from plenum.common.throughput_measurements import RevivalSpikeResistantEMAThroughputMeasurement
from plenum.common.util import getMaxFailures
from plenum.test.helper import sdk_send_random_and_check, assertExp, sdk_get_and_check_replies
from plenum.test.node_catchup.helper import waitNodeDataEquality

from plenum.test.pool_transactions.conftest import sdk_node_theta_added
from plenum.test.pool_transactions.helper import sdk_add_new_nym, prepare_new_node_data, prepare_node_request, \
    sdk_sign_and_send_prepared_request, create_and_start_new_node
from plenum.test.test_node import checkNodesConnected, TestNode
from stp_core.loop.eventually import eventually

nodeCount = 6


def _send_txn_for_creating_node(looper, sdk_pool_handle, sdk_wallet_steward, tdir, new_node_name, clientIp,
                                clientPort, nodeIp, nodePort, bls_key, sigseed, key_proof):
    new_steward_name = "testClientSteward"
    new_steward_wallet_handle = sdk_add_new_nym(looper,
                                                sdk_pool_handle,
                                                sdk_wallet_steward,
                                                alias=new_steward_name,
                                                role=STEWARD_STRING)

    # filling node request
    _, steward_did = new_steward_wallet_handle
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=new_node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             services=[VALIDATOR],
                             key_proof=key_proof))

    # sending request using 'sdk_' functions
    request_couple = sdk_sign_and_send_prepared_request(looper, new_steward_wallet_handle,
                                                        sdk_pool_handle, node_request)

    # waiting for replies
    sdk_get_and_check_replies(looper, [request_couple])


def test_catchup_after_replica_addition(looper, sdk_pool_handle, txnPoolNodeSet,
                                        sdk_wallet_steward, tdir, tconf, allPluginsPath):
    view_no = txnPoolNodeSet[-1].viewNo
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 1)
    waitNodeDataEquality(looper, *txnPoolNodeSet)

    # create node
    new_node_name = "Theta"
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, new_node_name)
    new_node = create_and_start_new_node(looper=looper, node_name=new_node_name,
                                         tdir=tdir, sigseed=sigseed,
                                         node_ha=(nodeIp, nodePort), client_ha=(clientIp, clientPort),
                                         tconf=tconf, auto_start=True, plugin_path=allPluginsPath,
                                         nodeClass=TestNode)

    _send_txn_for_creating_node(looper, sdk_pool_handle, sdk_wallet_steward, tdir, new_node_name, clientIp,
                                clientPort, nodeIp, nodePort, bls_key, sigseed, key_proof)

    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    looper.run(eventually(lambda: assertExp(n.viewNo == view_no + 1 for n in txnPoolNodeSet)))
    waitNodeDataEquality(looper, *txnPoolNodeSet)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 1)
    waitNodeDataEquality(looper, *txnPoolNodeSet, exclude_from_check=['check_last_ordered_3pc'])
