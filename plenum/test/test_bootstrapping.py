import pytest
from stp_core.network.exceptions import RemoteNotFound
from stp_core.common.log import getlogger

from plenum.test.helper import msgAll, sendMessageToAll
from plenum.test.test_node import checkNodesConnected

logger = getlogger()
nodeCount = 5


# noinspection PyIncorrectDocstring
def testKeyShareParty(looper, txnPoolNodeSet, tdir_for_func, tconf_for_func):
    """
    connections to all nodes should be successfully established when key
    sharing is enabled.
    """

    logger.debug("-----sharing keys-----")
    looper.run(checkNodesConnected(txnPoolNodeSet))

    logger.debug("-----key sharing done, connect after key sharing-----")
    looper.run(checkNodesConnected(txnPoolNodeSet),
               msgAll(txnPoolNodeSet))
    for node in txnPoolNodeSet:
        node.stop()


# noinspection PyIncorrectDocstring
def testConnectWithoutKeySharingFails(looper, txnPoolNodeSetNotStarted):
    """
    attempts at connecting to nodes when key sharing is disabled must fail
    """
    with pytest.raises(RemoteNotFound):
        sendMessageToAll(txnPoolNodeSetNotStarted, txnPoolNodeSetNotStarted[0])
