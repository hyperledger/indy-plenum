import pytest
from stp_core.network.exceptions import PublicKeyNotFoundOnDisk
from stp_core.common.log import getlogger
from plenum.test.greek import genNodeNames

from stp_core.loop.looper import Looper
from plenum.test.helper import msgAll
from plenum.test.test_node import TestNodeSet, checkNodesConnected, genNodeReg

logger = getlogger()

whitelist = ['public key from disk', 'verification key from disk',
             'doesnt have enough info to connect']
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


# noinspection PyIncorrectDocstring
@pytest.mark.skip(reason='get rid of registry pool')
def testConnectWithoutKeySharingFails(tdir_for_func, tconf_for_func):
    """
    attempts at connecting to nodes when key sharing is disabled must fail
    """
    nodeNames = genNodeNames(5)

    with pytest.raises(PublicKeyNotFoundOnDisk):
        with TestNodeSet(tconf_for_func, names=nodeNames, tmpdir=tdir_for_func,
                         keyshare=False) as nodes:
            with Looper(nodes) as looper:
                looper.run()
