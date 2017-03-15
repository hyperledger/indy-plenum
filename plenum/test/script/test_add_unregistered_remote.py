from copy import copy
import pytest

from plenum.common.looper import Looper
from plenum.test.helper import msgAll, randomText
from plenum.common.log import getlogger
from plenum.common.exceptions import RemoteNotFound
from plenum.common.types import CLIENT_STACK_SUFFIX
from plenum.common.port_dispenser import genHa
from plenum.test.test_node import TestNodeSet, checkNodesConnected, genNodeReg, TestNode
from plenum.common.types import NodeDetail


logger = getlogger()


def testAddUnregisteredRemote(tdir_for_func):
    nodeReg = genNodeReg(5)

    logger.debug("-----sharing keys-----")
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as looper:
            for n in nodeSet:
                n.startKeySharing()
            looper.run(checkNodesConnected(nodeSet))

    logger.debug("-----key sharing done, connect after key sharing-----")
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir_for_func) as nodeSet:
        with Looper(nodeSet) as loop:
            loop.run(checkNodesConnected(nodeSet),
                     msgAll(nodeSet))
        for nodeName, node in nodeSet.nodes.items():
            assert len(node.nodestack.spylog) == 0

    name = randomText(20)
    ha = genHa()
    cliname = name + CLIENT_STACK_SUFFIX
    cliha = genHa()
    faultyNodeReg = copy(nodeReg)
    faultyNodeReg.update({name: NodeDetail(ha, cliname, cliha)})
    unregisteredNode = TestNode(name=name, ha=ha, cliname=cliname, cliha=cliha,
                                nodeRegistry=copy(faultyNodeReg), basedirpath=tdir_for_func,
                                primaryDecider=None, pluginPaths=None)
    logger.debug("-----can not connect after adding unregistered node-----")
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir_for_func) as nodeSet:
        nodeSet.nodes[name] = unregisteredNode
        with Looper(nodeSet) as loop:
            with pytest.raises(RemoteNotFound) as e:
                loop.run(checkNodesConnected(nodeSet), msgAll(nodeSet))
            for nodeName, node in nodeSet.nodes.items():
                if node.name != unregisteredNode.name:
                    assert len(node.nodestack.spylog) > 0
