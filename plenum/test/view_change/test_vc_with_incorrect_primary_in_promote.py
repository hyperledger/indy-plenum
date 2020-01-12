import pytest as pytest

from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.constants import STEWARD_STRING, INSTANCE_CHANGE

from plenum.test.delayers import cDelay, ppDelay, pDelay, nv_delay, icDelay
from plenum.test.helper import waitForViewChange, checkViewNoForNodes
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node, sdk_add_new_nym, prepare_new_node_data, \
    prepare_node_request, sdk_sign_and_send_prepared_request, create_and_start_new_node
from plenum.test.stasher import delay_rules_without_processing, delay_rules
from plenum.test.test_node import checkNodesConnected, TestNode, ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change_complete


nodeCount = 7


def test_finish_view_change_with_incorrect_primaries_list2(looper, txnPoolNodeSet, sdk_pool_handle,
                                                          sdk_wallet_steward, tdir, tconf, allPluginsPath):
    view_no = txnPoolNodeSet[-1].viewNo

    # create new steward
    new_steward_for_zeta = sdk_add_new_nym(looper,
                                           sdk_pool_handle,
                                           sdk_wallet_steward,
                                           alias="new_steward_for_zeta",
                                           role=STEWARD_STRING)

    # Force 5 view changes so that we have viewNo == 5 and Zeta as the primary.
    for _ in range(5):
        ensure_view_change_complete(looper, txnPoolNodeSet)
        waitForViewChange(looper, txnPoolNodeSet, view_no + 1)
        ensureElectionsDone(looper, txnPoolNodeSet)
        view_no = checkViewNoForNodes(txnPoolNodeSet)

    # Add a New node but don't allow Alpha to be aware it. We do not want it in Alpha's node registry.
    for n in txnPoolNodeSet:
        n.nodeIbStasher.delay(icDelay())

    _, epsilon = sdk_add_new_steward_and_node(looper, sdk_pool_handle, sdk_wallet_steward,
                                               'New_Steward', 'Epsilon',
                                               tdir, tconf, allPluginsPath=allPluginsPath)
    txnPoolNodeSet.append(epsilon)

    # nodes_sans_alpha = txnPoolNodeSet[1:]
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # for n in txnPoolNodeSet:
    #     n.nodeIbStasher.delay(cDelay())
    epsilon.nodeIbStasher.delay(cDelay())

    for n in txnPoolNodeSet:
        n.nodeIbStasher.reset_delays_and_process_delayeds(INSTANCE_CHANGE)

    # All node performed another VIEW_CHANGE so we should have viewNo == 6 and Eta as the primary.
    # Next in line to be the primary is New, and all nodes except Alpha have the New node in node registry.
    waitForViewChange(looper, txnPoolNodeSet, view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)

    ensure_view_change_complete(looper, txnPoolNodeSet)
    new_node = add_new_node_without_wait(looper,
                                         sdk_pool_handle,
                                         new_steward_for_zeta, tconf, tdir, allPluginsPath, 'Zeta')
    # ensure_all_nodes_have_same_data(looper, [*txnPoolNodeSet, new_node])
    ensure_all_nodes_have_same_data(looper, [*txnPoolNodeSet[:-1], new_node])
    txnPoolNodeSet.append(new_node)

    # for n in txnPoolNodeSet[:nodeCount]:
    #     n.nodeIbStasher.reset_delays_and_process_delayeds()
    waitForViewChange(looper, txnPoolNodeSet[:-1], view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet[:-1])
    epsilon.nodeIbStasher.reset_delays_and_process_delayeds()
    sdk_ensure_pool_functional(looper, txnPoolNodeSet[:-1], sdk_wallet_steward, sdk_pool_handle)



def test_finish_view_change_with_incorrect_primaries_list(looper, txnPoolNodeSet, sdk_pool_handle,
                                                          sdk_wallet_steward, tdir, tconf, allPluginsPath):
    view_no = txnPoolNodeSet[-1].viewNo
    lagging_node = txnPoolNodeSet[-1]
    fast_nodes = txnPoolNodeSet[:-1]

    # create new steward
    new_steward_for_zeta = sdk_add_new_nym(looper,
                                           sdk_pool_handle,
                                           sdk_wallet_steward,
                                           alias="new_steward_for_zeta",
                                           role=STEWARD_STRING)

    # Force 5 view changes so that we have viewNo == 5 and Zeta as the primary.
    for _ in range(5):
        ensure_view_change_complete(looper, txnPoolNodeSet)
        waitForViewChange(looper, txnPoolNodeSet, view_no + 1)
        ensureElectionsDone(looper, txnPoolNodeSet)
        view_no = checkViewNoForNodes(txnPoolNodeSet)

    with delay_rules_without_processing(lagging_node.nodeIbStasher, icDelay(), cDelay()):

        _, epsilon = sdk_add_new_steward_and_node(looper, sdk_pool_handle, sdk_wallet_steward,
                                                   'New_Steward', 'new_node1',
                                                   tdir, tconf, allPluginsPath=allPluginsPath)
        txnPoolNodeSet.append(epsilon)
        fast_nodes.append(epsilon)

        # nodes_sans_alpha = txnPoolNodeSet[1:]
        looper.run(checkNodesConnected(fast_nodes))
        ensure_all_nodes_have_same_data(looper, fast_nodes)

        waitForViewChange(looper, fast_nodes, view_no + 1)
        ensureElectionsDone(looper, fast_nodes)

    ensure_view_change_complete(looper, fast_nodes)
    print(view_no + 1)
    print(txnPoolNodeSet[0].viewNo)
    # new_node = add_new_node_without_wait(looper,
    #                                      sdk_pool_handle,
    #                                      new_steward_for_zeta, tconf, tdir, allPluginsPath, 'Zeta')
    _, new_node = sdk_add_new_steward_and_node(looper, sdk_pool_handle, sdk_wallet_steward,
                                               'New_Zeta_Steward', 'new_node2',
                                               tdir, tconf, allPluginsPath=allPluginsPath)
    # ensure_all_nodes_have_same_data(looper, [*txnPoolNodeSet, new_node])
    ensure_all_nodes_have_same_data(looper, [*fast_nodes, new_node])
    txnPoolNodeSet.append(new_node)

    # for n in txnPoolNodeSet[:nodeCount]:
    #     n.nodeIbStasher.reset_delays_and_process_delayeds()
    waitForViewChange(looper, txnPoolNodeSet[:-1], view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet[:-1])
    epsilon.nodeIbStasher.reset_delays_and_process_delayeds()
    sdk_ensure_pool_functional(looper, txnPoolNodeSet[:-1], sdk_wallet_steward, sdk_pool_handle)


def add_new_node_without_wait(looper,
                              sdk_pool_handle,
                              new_steward_wallet_handle, tconf, tdir, allPluginsPath, node_name):
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, node_name)

    # create node request to add new demote node
    _, steward_did = new_steward_wallet_handle
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             services=[],
                             key_proof=key_proof))
    request1 = sdk_sign_and_send_prepared_request(looper, new_steward_wallet_handle,
                                                  sdk_pool_handle, node_request)

    return create_and_start_new_node(looper, node_name, tdir, sigseed,
                                     (nodeIp, nodePort), (clientIp, clientPort),
                                     tconf, True, allPluginsPath,
                                     TestNode, configClass=PNodeConfigHelper)
