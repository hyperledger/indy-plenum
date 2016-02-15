
# TODO Test that a node cannot start if either of its ports are already in use
# import socket
# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# result = sock.connect_ex(('127.0.0.1', 7560))
# if result == 0:
#    print("Port is open")
# else:
#    print("Port is not open")
import pytest

from zeno.common.stacked import HA
from zeno.server.node import NodeDetail


# noinspection PyIncorrectDocstring
@pytest.fixture('module')
def overlapNodePorts(nodeReg):
    """
    From the given node registry, make Alpha and Beta run on the same port.
    """
    print(nodeReg)
    A = nodeReg['Alpha']
    betaPort = nodeReg['Beta'].ha.port
    betaClientPort = nodeReg['Beta'].cliha.port
    # nodeReg['Alpha'] = NodeDetail(HA(A.ha.host, A.ha.port), A.cliname, HA(A.cliha.host, A.cliha.port))
    nodeReg['Alpha'] = NodeDetail(HA(A.ha.host, betaPort), A.cliname, HA(A.cliha.host, A.cliha.port))



# noinspection PyIncorrectDocstring
def testOverlappingNodePorts(up):
    """
    With the Alpha and Beta nodes running on the same port, the consensus pool
     should still be able to come up.
    """
    pass

# TODO why is count initially moving from None to 1 when there isn't a connection???
