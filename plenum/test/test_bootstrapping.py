import logging

from plenum.common.exceptions import RemoteNotFound
from plenum.server.client_authn import SimpleAuthNr
from plenum.test.greek import genNodeNames

from plenum.common.looper import Looper
from plenum.test.helper import genNodeReg, checkNodesConnected, TestNodeSet, \
    RemoteState, msgAll


# noinspection PyIncorrectDocstring
def testKeyShareParty(tdir_for_func):
    """
    connections to all nodes should be successfully established when key
    sharing is enabled.
    """
    nodeReg = genNodeReg(5)

    logging.debug("-----sharing keys-----")
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:
            for n in nodeSet:
                n.startKeySharing()
            looper.run(checkNodesConnected(nodeSet))

    logging.debug("-----key sharing done, connect after key sharing-----")
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as loop:
            loop.run(checkNodesConnected(nodeSet),
                     msgAll(nodeSet))


# noinspection PyIncorrectDocstring
def testConnectWithoutKeySharingFails(tdir_for_func):
    """
    attempts at connecting to nodes when key sharing is disabled must fail
    """
    nodeNames = genNodeNames(5)
    with TestNodeSet(names=nodeNames, tmpdir=tdir_for_func,
                     keyshare=False) as nodes:
        with Looper(nodes) as looper:
            try:
                looper.run(
                        checkNodesConnected(nodes,
                                            RemoteState(None, None, None)))
            except RemoteNotFound:
                pass
            except KeyError as ex:
                assert [n for n in nodeNames
                        if n == ex.args[0]]
            except Exception:
                raise
