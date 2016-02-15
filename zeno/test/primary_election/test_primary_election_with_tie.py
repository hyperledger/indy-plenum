import logging

import pytest
from zeno.test.eventually import eventually
from zeno.test.helper import TestNodeSet, checkPoolReady, \
    checkProtocolInstanceSetup, delay

from zeno.common.request_types import Nomination
from zeno.test.primary_election.helpers import checkNomination

nodeCount = 4


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
            logging.debug("replica {} {} with votes {}".
                          format(replica.name, replica.instId,
                                 node.elector.nominations.get(instId, {})))

    logging.debug("Check nomination")
    # Checking whether Node A nominated itself
    looper.run(eventually(checkNomination, A, A.name, retryWait=1, timeout=10))

    # Checking whether Node B nominated itself
    looper.run(eventually(checkNomination, B, B.name, retryWait=1, timeout=10))

    # Checking whether Node C nominated Node A
    looper.run(eventually(checkNomination, C, A.name, retryWait=1, timeout=10))

    # Checking whether Node D nominated Node D
    looper.run(eventually(checkNomination, D, B.name, retryWait=1, timeout=10))

    # No node should be primary
    for node in nodeSet.nodes.values():
        assert node.hasPrimary is False

    for node in nodeSet.nodes.values():
        node.resetDelays()

    # TODO Check for spylog of `eatReelection` after putting spy on eaters too

    checkProtocolInstanceSetup(looper=looper, nodes=nodeSet, retryWait=1,
                               timeout=60)

    # TODO testPrimaryElectionWithTieButOneNodeDoesntKnowIt
    # TODO add E back in after election is complete, or even before it's
    # complete (E jumps in right in the middle of an election)
    #       When E joins, it needs to be able to ping the others for the
    #       current state (A could send to E the votes of the other nodes,
    #       so E would know if B is being maliciious), and the others respond,
    #       and E can make a determination based on those responses, even if
    #       one is malicious.

    # TODO We need to trap for the case when a node is being inconsistent. For example:
    """
        A is malicious
        A sends NOMINATE(id, C) to B
        A sends NOMINATE(id, B) to C and D
        B forwards A's signed NOMINATE(id, C) to C and D
        C forwards A's signed NOMINATE(id, B) to B and D
        D forwards A's signed NOMINATE(id, B) to B and C
        C and D and B can all see that A is being inconsistent, and they all blacklist A

        If A is using different ID's for each record, then we can see that as well.
    """
