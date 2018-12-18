import pytest
from plenum.test.helper import sdk_send_random_request

from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import Commit
from plenum.server.replica import Replica
from plenum.test.delayers import delayerMsgTuple
from plenum.test.test_node import TestNode
from plenum.test import waits

nodeCount = 4

faultyNodes = 1


@pytest.fixture()
def configNodeSet(txnPoolNodeSet):
    A, B, C, D = txnPoolNodeSet
    # Nodes C and D delay Commit request from node A for protocol instance 0
    for n in [C, D]:
        n.nodeIbStasher.delay(delayerMsgTuple(30,
                                              Commit,
                                              senderFilter=A.name,
                                              instFilter=0))
    return txnPoolNodeSet


def testMsgFromInstanceDelay(configNodeSet, looper,
                             sdk_pool_handle, sdk_wallet_client):
    A, B, C, D = configNodeSet

    sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

    def getCommits(node: TestNode, instId: int):
        replica = node.replicas[instId]  # type: Replica
        return list(replica.commits.values())

    def checkPresence():
        for node in [C, D]:
            commReqs = getCommits(node, 0)
            assert len(commReqs) > 0
            assert Replica.generateName(A.name, 0) not in commReqs[0][0]
            commReqs = getCommits(node, 1)
            assert len(commReqs) > 0
            assert Replica.generateName(A.name, 1) in commReqs[0][0]

    numOfNodes = len(configNodeSet)
    timeout = waits.expectedClientRequestPropagationTime(numOfNodes)
    looper.run(eventually(checkPresence, retryWait=.5, timeout=timeout))
