import pytest

heartbeat_freq = 2


@pytest.fixture(scope="module")
def tconf(tconf):
    tconf.UseZStack = True
    return tconf


def test_ping_reconnection(tconf, looper, txnPoolNodeSet):
    node1 = txnPoolNodeSet[0]
    node2 = txnPoolNodeSet[1]
    socket_before = node1.nodestack._remotes['Beta'].socket
    for i in range(0, tconf.PINGS_BEFORE_SOCKET_RECONNECTION + 1):
        node2.nodestack.sendPingPong(node2.nodestack._remotes['Alpha'])
    looper.runFor(6 * heartbeat_freq)
    socket_after = node1.nodestack._remotes['Beta'].socket
    # reconnection by pings is disabled now
    assert socket_before == socket_after
