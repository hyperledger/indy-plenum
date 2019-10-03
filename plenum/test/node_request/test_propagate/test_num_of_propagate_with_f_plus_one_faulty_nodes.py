import pytest
from stp_core.common.util import adict

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
def setup(txnPoolNodeSet):
    E = txnPoolNodeSet[-3]
    G = txnPoolNodeSet[-2]
    Z = txnPoolNodeSet[-1]
    for node in E, G, Z:
        makeNodeFaulty(node, changesRequest)
    return adict(faulties=(E, G, Z))


@pytest.fixture(scope="module")
def afterElection(setup):
    for n in setup.faulties:
        for r in n.replicas.values():
            assert not r.isPrimary


def testNumOfPropagateWithFPlusOneFaults(afterElection, propagated1):
    pass
