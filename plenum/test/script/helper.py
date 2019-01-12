import pytest
from plenum.common.constants import VALIDATOR

from plenum.test.pool_transactions.helper import sdk_send_update_node, sdk_pool_refresh
from stp_core.common.log import getlogger
from plenum.common.util import hexToFriendly
from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.test_node import TestNode, checkNodesConnected, \
    ensureElectionsDone
from stp_core.network.port_dispenser import genHa
from plenum.common.config_helper import PNodeConfigHelper

logger = getlogger()


@pytest.yield_fixture(scope="module")
def looper(txnPoolNodesLooper):
    yield txnPoolNodesLooper


def changeNodeHa(looper, txnPoolNodeSet,
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
    subjectedNode.stop()
    looper.removeProdable(subjectedNode)

    # start node with new HA
    config_helper = PNodeConfigHelper(subjectedNode.name, tconf, chroot=tdir)
    restartedNode = TestNode(subjectedNode.name,
                             config_helper=config_helper,
                             config=tconf, ha=nodeStackNewHA,
                             cliha=clientStackNewHA)
    looper.add(restartedNode)
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
