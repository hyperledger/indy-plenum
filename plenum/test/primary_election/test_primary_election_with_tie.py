import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.messages.node_messages import Nomination
from plenum.test.delayers import delay
from plenum.test.primary_election.helpers import checkNomination
from plenum.test.test_node import TestNodeSet, checkPoolReady, \
    checkProtocolInstanceSetup
from plenum.test import waits


nodeCount = 4


logger = getlogger()


@pytest.fixture()
def electTieFixture(startedNodes: TestNodeSet):
    A, B, C, D = startedNodes.nodes.values()

    for node in [C, D]:
        node.delaySelfNomination(10)

    delay(Nomination, frm=A, to=(B, D), howlong=5)
    delay(Nomination, frm=B, to=(A, C), howlong=5)
    delay(Nomination, frm=C, to=(D, B), howlong=5)
    delay(Nomination, frm=D, to=(C, A), howlong=5)


# noinspection PyIncorrectDocstring
@pytest.mark.skip('Nodes use round robin primary selection')
def testPrimaryElectionWithTie(electTieFixture, looper, keySharedNodes):
    """
    Primary selection (Rainy Day)
    A, B, C, D, E
    A, B, C, D startup. E is lagging.
    A sees the minimum number of nodes, and then sends Nominate(A)
    At the same exact time, B sees the minimum number of nodes, and then sends out Nominate(B)
    A sees B sending Nominate(B), but it has already nominated itself, so it does nothing
    B sees A sending Nominate(A), but it has already nominated itself, so it does nothing
    C sees A sending Nominate(A), and sends Nominate(A)
    D sees B sending Nominate(B), and sends Nominate(B)
    There's a split. C and A think A is the primary, B and D think B is the primary
    All nodes can see that there is a split. Each sends out Reelection([A,B])

    A and B both see Reelection([A,B]) from themselves as well as the other 3 (the number from others should be at least f+1),

    1. they wait a random amount of time (between 0 and 2 seconds),
    2. they each send out a Nominate(self)

    Voting is repeated until we have a good election.
    """

    # TODO optimize the sending messages in batches, for example, we don't
    #     send messages more often than 400 milliseconds. Once those 400
    #     millis have passed, we send the several queued messages in one
    #     batch.

    nodeSet = keySharedNodes
    A, B, C, D = nodeSet.nodes.values()

    checkPoolReady(looper, nodeSet.nodes.values())

    for node in nodeSet.nodes.values():
        for instId, replica in enumerate(node.elector.replicas):
            logger.debug("replica {} {} with votes {}".
                         format(replica.name, replica.instId,
                                node.elector.nominations.get(instId, {})))

    nominationTimeout = waits.expectedPoolNominationTimeout(len(nodeSet))
    logger.debug("Check nomination")
    # Checking whether Node A nominated itself
    looper.run(eventually(checkNomination, A, A.name,
                          retryWait=1, timeout=nominationTimeout))

    # Checking whether Node B nominated itself
    looper.run(eventually(checkNomination, B, B.name,
                          retryWait=1, timeout=nominationTimeout))

    # Checking whether Node C nominated Node A
    looper.run(eventually(checkNomination, C, A.name,
                          retryWait=1, timeout=nominationTimeout))

    # Checking whether Node D nominated Node D
    looper.run(eventually(checkNomination, D, B.name,
                          retryWait=1, timeout=nominationTimeout))

    # No node should be primary
    for node in nodeSet.nodes.values():
        assert node.hasPrimary is False

    for node in nodeSet.nodes.values():
        node.resetDelays()

    checkProtocolInstanceSetup(looper=looper, nodes=nodeSet, retryWait=1)
