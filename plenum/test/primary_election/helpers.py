from plenum.server.replica import Replica
from plenum.test.test_node import TestNode


def checkNomination(node: TestNode, nomineeName: str):
    matches = [replica.name for instId, replica in enumerate(node.elector.replicas) if
               node.elector.didReplicaNominate(instId) is True and
               replica.name in node.elector.nominations[instId] and
               node.elector.nominations[instId][replica.name] ==
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
    for instId, replica in enumerate(node.elector.replicas):
        name = Replica.generateName(node.name, instId)
        if node.elector.nominations.get(instId, {}).get(name, None) == name:
            return instId