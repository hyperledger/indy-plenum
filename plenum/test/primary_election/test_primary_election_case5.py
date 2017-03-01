import logging

import pytest
from plenum.common.types import Primary, Nomination
from plenum.common.log import getlogger

from plenum.server.replica import Replica
from plenum.server.suspicion_codes import Suspicions
from plenum.test.test_node import TestNodeSet, checkNodesConnected, \
    ensureElectionsDone
from plenum.test.delayers import delayerMsgTuple

nodeCount = 4
whitelist = ['because already got primary declaration',
             Suspicions.DUPLICATE_PRI_SENT.reason,
             'doing nothing for now',
             'know how to handle it']


logger = getlogger()


@pytest.fixture()
def case5Setup(startedNodes: TestNodeSet):
    A, B, C, D = startedNodes.nodes.values()

    # Node B delays self nomination so A's nomination reaches everyone
    B.delaySelfNomination(30)
    # Node B delays NOMINATE from Node A, B, C since it needs to send PRIMARY
    # messages so it should not get any `NOMINATE` which might make it do
    # Primary declarations much before we need it too

    # A, C and D should not blacklist B since we are trying to check if
    # multiple primary declarations from the same node have any impact on
    # the election
    for node in A, C, D:
        node.whitelistNode(B.name, Suspicions.DUPLICATE_PRI_SENT.code)

    for node in [A, C, D]:
        B.nodeIbStasher.delay(delayerMsgTuple(30,
                                              Nomination,
                                              senderFilter=node.name,
                                              instFilter=0))

    for node in [C, D]:
        # Nodes C and D delay NOMINATE from node A
        node.nodeIbStasher.delay(delayerMsgTuple(5,
                                                 Nomination,
                                                 senderFilter=A.name,
                                                 instFilter=0))
        # Also Nodes C and D are slow so they will not nominate themselves
        node.delaySelfNomination(25)


# noinspection PyIncorrectDocstring
def testPrimaryElectionCase5(case5Setup, looper, keySharedNodes):
    """
    Case 5 - A node making primary declarations for a multiple other nodes.
    Consider 4 nodes A, B, C, and D. Lets say node B is malicious and
    declares node C as primary to all nodes.
    Again node B declares node D as primary to all nodes.
    """
    nodeSet = keySharedNodes
    A, B, C, D = nodeSet.nodes.values()

    looper.run(checkNodesConnected(nodeSet))

    BRep = Replica.generateName(B.name, 0)
    CRep = Replica.generateName(C.name, 0)
    DRep = Replica.generateName(D.name, 0)

    # Node B first sends PRIMARY msgs for Node C to all nodes
    B.send(Primary(CRep, 0, B.viewNo))
    # Node B sends PRIMARY msgs for Node D to all nodes
    B.send(Primary(DRep, 0, B.viewNo))

    # Ensure elections are done
    ensureElectionsDone(looper=looper, nodes=nodeSet)

    # All nodes from node A, node C, node D(node B is malicious anyway so not
    # considering it) should have primary declarations for node C from node B
    #  since node B first nominated node C
    for node in [A, C, D]:
        logger.debug(
            "node {} should have primary declaration for C from node B"
            .format(node))
        assert node.elector.primaryDeclarations[0][BRep] == CRep
