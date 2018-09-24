from plenum.common.messages.node_messages import Nomination, Primary
from plenum.server.replica import Replica
from plenum.test.test_node import TestNode


def checkNomination(node: TestNode, nomineeName: str):
    matches = [replica.name for instId, replica in node.elector.replicas if
               node.elector.didReplicaNominate(instId) is True and
               replica.name in node.elector.nominations[instId] and
               node.elector.nominations[instId][replica.name][0] ==
               Replica.generateName(nomineeName, instId)]
    assert len(matches) > 0
    return matches[0]


# TODO Think of a better name for this function
def getSelfNominationByNode(node: TestNode) -> int:
    """
    This function returns the index of the protocol instance for which it nominated itself
    @param node: the node
    @return: the protocol instance index
    """
    for instId, replica in node.elector.replicas:
        name = Replica.generateName(node.name, instId)
        if node.elector.nominations.get(instId, {}).get(name, [None, ])[
            0] == name:
            return instId


def nominationByNode(name: str, byNode: TestNode, instId: int):
    return Nomination(name, instId, byNode.viewNo,
                      byNode.replicas[instId].lastOrderedPPSeqNo[1])


def primaryByNode(name: str, byNode: TestNode, instId: int):
    return Primary(name, instId, byNode.viewNo,
                   byNode.replicas[instId].lastOrderedPPSeqNo[1])
