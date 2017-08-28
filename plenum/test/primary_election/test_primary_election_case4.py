import pytest

from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import Primary
from plenum.server.suspicion_codes import Suspicions
from plenum.test import waits
from plenum.test.primary_election.helpers import primaryByNode
from plenum.test.test_node import TestNodeSet, checkNodesConnected, \
    ensureElectionsDone

nodeCount = 4
whitelist = ['because already got primary declaration',
             Suspicions.DUPLICATE_PRI_SENT.reason,
             'doing nothing for now',
             'know how to handle it']

delaySelfNomination = 5


@pytest.fixture()
def case4Setup(keySharedNodes: TestNodeSet):
    allNodes = keySharedNodes.nodes.values()
    A, B, C, D = allNodes

    # Delay each of the nodes A, B and C's self nomination so Node B gets to
    # declare a primary before a primary is selected
    for n in (A, B, C):
        n.delaySelfNomination(delaySelfNomination)

    # Node D is slow so it nominates itself after long time
    D.delaySelfNomination(25)

    # A, C and D should not blacklist B since we are trying to check if
    # multiple primary declarations from the same node have any impact on
    # the election
    for node in A, C, D:
        node.whitelistNode(B.name, Suspicions.DUPLICATE_PRI_SENT.code)

    return allNodes


# noinspection PyIncorrectDocstring
@pytest.mark.skip('Nodes use round robin primary selection')
def testPrimaryElectionCase4(case4Setup, looper):
    """
    Case 4 - A node making multiple primary declarations for a particular node.
    Consider 4 nodes A, B, C and D. Lets say node B is malicious and is
    repeatedly declaring Node D as primary
    """
    allNodes = case4Setup
    A, B, C, D = allNodes

    looper.run(checkNodesConnected(allNodes))

    # Node B sends multiple declarations of node D's 0th protocol instance as
    # primary to all nodes
    for i in range(5):
        # B.send(Primary(D.name, 0, B.viewNo))
        B.send(primaryByNode(D.name, B, 0))

    # No node from node A, node C, node D(node B is malicious anyway so not
    # considering it) should have more than one primary declaration for node
    # D since node D is slow. The one primary declaration for node D,
    # that nodes A, C and D might have would be because of node B
    def x():
        primDecs = [p[0] for p in node.elector.primaryDeclarations[0].values()]
        assert primDecs.count(D.name) <= 1

    # also have to take into account the catchup procedure
    timeout = waits.expectedPoolNominationTimeout(len(allNodes)) + \
        waits.expectedPoolCatchupTime(len(allNodes))

    for node in (A, C, D):
        looper.run(eventually(x, retryWait=.5, timeout=timeout))

    timeout = waits.expectedPoolElectionTimeout(
        len(allNodes)) + delaySelfNomination
    ensureElectionsDone(looper=looper, nodes=allNodes, customTimeout=timeout)

    # Node D should not have any primary replica
    assert not D.hasPrimary
