import pytest
from stp_core.common.util import adict

from plenum.test.malicious_behaviors_node import makeNodeFaulty, changesRequest

nodeCount = 7
faultyNodes = 2  # Max failures system can tolerate
whitelist = ['doing nothing for now',
             'InvalidSignature']
"""
    A gets REQ
    B gets REQ, but is malicious
    C doesn't get REQ
    D doesn't get REQ

    A sends PROP to B, C, D
    B quietly swallows it

    C gets PROP from A, and sends PROP to A, B and D; C's PROP_count = 1
    D gets PROP from A, and sends PROP to A, B and C; D's PROP_count = 1
    B quietly swallows C's
    B quietly swallows D's

    A gets PROP from C, and sees that it (A) has already sent a PROP, so it doesn't send again,
        A's PROP_count = 2
        forwards to replicas

    A gets PROP from D, and sees that it (A) has already sent a PROP, so it doesn't send again,
        A's PROP_count = 3
        already forwarded, so does nothing

    D gets PROP from C, and sees that it (D) has already sent a PROP, so it doesn't send again,
        D's PROP_count = 2
        forwards to replicas
    C gets PROP from D, and sees that it (C) has already sent a PROP, so it doesn't send again,
        C's PROP_count = 2
        forwards to replicas

"""


@pytest.fixture(scope="module")
def setup(txnPoolNodeSet):
    # Making nodes faulty such that no primary is chosen
    G = txnPoolNodeSet[-2]
    Z = txnPoolNodeSet[-1]
    for node in G, Z:
        makeNodeFaulty(node, changesRequest)
        # node.delaySelfNomination(10)
    return adict(faulties=(G, Z))


@pytest.fixture(scope="module")
def afterElection(setup):
    for n in setup.faulties:
        for r in n.replicas.values():
            assert not r.isPrimary


def testNumOfSufficientPropagate(afterElection, noRetryReq, propagated1):
    pass
