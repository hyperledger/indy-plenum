import pytest

from plenum.test.test_node import getPrimaryReplica


@pytest.mark.skip(reason="SOV-556. Test implementation pending, "
                         "although bug fixed")
def testPrimaryForfeit(looper, nodeSet, up, client1, wallet1):
    """
    The primary of master protocol instance of the pool forfeits the primary
    status by triggering an election and not nominating itself
    """
    pr = getPrimaryReplica(nodeSet, instId=0)
    prNode = pr.node
    # TODO: Incomplete
    raise NotImplementedError
