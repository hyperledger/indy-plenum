from collections import deque
import pytest

from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import prepStacks, CounterMsgsHandler
from stp_zmq.test.helper import genKeys
from stp_zmq.zstack import ZStack


@pytest.fixture()
def tconf(tconf):
    old_value = tconf.ZMQ_STASH_UNKNOWN_REMOTE_MSGS_QUEUE_SIZE
    tconf.ZMQ_STASH_UNKNOWN_REMOTE_MSGS_QUEUE_SIZE = 3
    yield tconf
    tconf.ZMQ_STASH_UNKNOWN_REMOTE_MSGS_QUEUE_SIZE = old_value


def test_stashing_unknown_remote_msgs(looper, tdir, tconf):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)

    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=None, restricted=False)

    beta_msg_handler = CounterMsgsHandler()
    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=beta_msg_handler.handler, restricted=False)

    prepStacks(looper, alpha, beta, connect=False)

    assert not alpha.hasRemote(beta.name)
    assert not alpha.isConnectedTo(beta.name)
    assert not beta.hasRemote(alpha.name)
    assert not beta.isConnectedTo(alpha.name)

    alpha.connect(name=beta.name, ha=beta.ha, verKeyRaw=beta.verKeyRaw, publicKeyRaw=beta.publicKeyRaw)
    alpha.getRemote(beta.name, beta.ha).setConnected()

    assert alpha.hasRemote(beta.name)
    assert alpha.isConnectedTo(beta.name)
    assert not beta.hasRemote(alpha.name)
    assert not beta.isConnectedTo(alpha.name)

    def check_unknown_remote_msg():
        assert len(beta._stashed_unknown_remote_msgs) == len(sent_msgs)
        for index, item in enumerate(sent_msgs):
            assert item == beta._stashed_unknown_remote_msgs[index][0]
            assert alpha.remotes['Beta'].socket.IDENTITY == beta._stashed_unknown_remote_msgs[index][1]

    sent_msgs = deque(maxlen=tconf.ZMQ_STASH_UNKNOWN_REMOTE_MSGS_QUEUE_SIZE)
    msg = 'message num: {}'
    for i in range(5):
        _msg = msg.format(i)
        alpha.send(_msg)
        sent_msgs.append(_msg)
        looper.run(eventually(check_unknown_remote_msg, retryWait=1, timeout=60))
