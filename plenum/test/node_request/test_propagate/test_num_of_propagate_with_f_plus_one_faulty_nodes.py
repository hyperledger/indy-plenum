import pytest
from plenum.common.util import adict

from plenum.test.malicious_behaviors_node import makeNodeFaulty, changesRequest

nodeCount = 7
# More faulty nodes(3) then system can tolerate(3)
faultyNodes = 3
whitelist = ['for InvalidSignature',
             'discarding message']
"""
When system has more than f + 1 faulty nodes,
Num of PROPAGATE messages must be less than sufficient (faultyNodes + 1)
"""


# Currently, all the nodes have same malicious
# behavior and should be chose randomly later.

@pytest.fixture(scope="module")
def setup(startedNodes):
    A = startedNodes.Alpha
    B = startedNodes.Beta
    G = startedNodes.Gamma
    for node in A, B, G:
        makeNodeFaulty(node, changesRequest)
        node.delaySelfNomination(10)
    return adict(faulties=(A, B, G))


@pytest.fixture(scope="module")
def afterElection(setup, up):
    for n in setup.faulties:
        for r in n.replicas:
            assert not r.isPrimary


def testNumOfPropagateWithFPlusOneFaults(afterElection, propagated1):
    pass
