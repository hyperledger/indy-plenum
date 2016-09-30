import pytest

from plenum.common.types import NodeDetail, HA


# noinspection PyIncorrectDocstring
@pytest.fixture('module')
def overlapNodePorts(nodeReg):
    """
    From the given node registry, make Alpha and Beta run on the same port.
    """
    A = nodeReg['Alpha']
    betaPort = nodeReg['Beta'].ha.port
    nodeReg['Alpha'] = NodeDetail(HA(A.ha.host, betaPort), A.cliname,
                                  HA(A.cliha.host, A.cliha.port))


# noinspection PyIncorrectDocstring
def testOverlappingNodePorts(up):
    """
    With the Alpha and Beta nodes running on the same port, the consensus pool
     should still be able to come up.
    """
    pass
