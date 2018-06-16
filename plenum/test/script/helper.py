import pytest
from plenum.common.constants import VALIDATOR

from plenum.test.pool_transactions.helper import sdk_send_update_node, sdk_pool_refresh, \
    disconnect_node_and_ensure_disconnected
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.util import hexToFriendly
from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check, create_node_inside_thread
from plenum.test.test_client import genTestClient
from plenum.test.test_node import TestNode, checkNodesConnected, \
    ensureElectionsDone
from stp_core.network.port_dispenser import genHa
from plenum.common.config_helper import PNodeConfigHelper

logger = getlogger()


@pytest.yield_fixture(scope="module")
def looper(txnPoolNodesLooper):
    yield txnPoolNodesLooper


def changeNodeHa(looper, txnPoolNodeSet, tdirWithClientPoolTxns,
                 tconf, shouldBePrimary, tdir,
                 sdk_pool_handle, sdk_wallet_stewards,
                 sdk_wallet_client):
    # prepare new ha for node and client stack
    subjectedNode = None
    node_index = None

    for nodeIndex, n in enumerate(txnPoolNodeSet):
        if shouldBePrimary == n.has_master_primary:
            subjectedNode = n
            node_index = nodeIndex
            break

    nodeStackNewHA, clientStackNewHA = genHa(2)
    logger.debug("change HA for node: {} to {}".format(
        subjectedNode.name, (nodeStackNewHA, clientStackNewHA)))

    # change HA
    sdk_wallet_steward = sdk_wallet_stewards[node_index]
    node_dest = hexToFriendly(subjectedNode.nodestack.verhex)
    sdk_send_update_node(looper, sdk_wallet_steward,
                         sdk_pool_handle,
                         node_dest, subjectedNode.name,
                         nodeStackNewHA[0], nodeStackNewHA[1],
                         clientStackNewHA[0], clientStackNewHA[1],
                         services=[VALIDATOR])

    # stop node for which HA will be changed
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            subjectedNode,
                                            stopNode=True)

    # start node with new HA
    restartedNode = create_node_inside_thread(TestNode,
                                              PNodeConfigHelper,
                                              subjectedNode.name,
                                              tconf,
                                              tdir,
                                              node_ha=nodeStackNewHA,
                                              client_ha=clientStackNewHA,
                                              allPluginsPath=None)
    txnPoolNodeSet[nodeIndex] = restartedNode
    looper.run(checkNodesConnected(txnPoolNodeSet, customTimeout=70))


    electionTimeout = waits.expectedPoolElectionTimeout(
        nodeCount=len(txnPoolNodeSet),
        numOfReelections=3)
    ensureElectionsDone(looper,
                        txnPoolNodeSet,
                        retryWait=1,
                        customTimeout=electionTimeout)

    sdk_pool_refresh(looper, sdk_pool_handle)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              8)
