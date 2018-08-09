import types

import pytest

from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, CounterMsgsHandler, prepStacks, MessageSender, connectStack
from stp_zmq.test.helper import genKeys, check_pong_received
from stp_zmq.zstack import ZStack

NUM_MSGS = 100


def patch_ping_pong(stack):
    origMethod = stack.handlePingPong

    stack.drop_ping = False
    stack.drop_pong = False
    stack.has_ping = set()

    def patchedHandlePingPong(self, msg, frm, ident):
        if self.drop_ping and msg == self.pingMessage:
            return
        if self.drop_pong and msg == self.pongMessage:
            return

        if msg == self.pingMessage:
            self.has_ping.add(frm)

        return origMethod(msg, frm, ident)

    stack.handlePingPong = types.MethodType(patchedHandlePingPong, stack)


def drop_pongs(stack):
    stack.drop_pong = True


def drop_pings(stack):
    stack.drop_ping = True


def check_all_received(looper, frm, to, to_msg_handler):
    looper.run(eventually(to_msg_handler.check_received_from,
                          frm.name, NUM_MSGS,
                          retryWait=1,
                          timeout=15))


def check_ping_received(looper, stack, frm):
    def do_check_ping():
        assert frm in stack.has_ping

    looper.run(eventually(do_check_ping))


def re_send_ping(alpha, beta):
    alpha.drop_ping = False
    alpha.drop_pong = False
    beta.sendPingPong(alpha.name, is_ping=True)


def re_send_pong(alpha, beta):
    alpha.drop_ping = False
    alpha.drop_pong = False
    beta.sendPingPong(alpha.name, is_ping=False)


def create_alpha(tdir, looper, name):
    alphaP = Printer(name)
    alpha = ZStack(name,
                   ha=genHa(),
                   basedirpath=tdir,
                   msgHandler=alphaP.print,
                   restricted=True)
    prepStacks(looper, alpha, connect=False)

    patch_ping_pong(alpha)

    return alpha


def create_beta(tdir, looper, name, start_stack=True):
    beta_msg_handler = CounterMsgsHandler()
    beta = ZStack(name,
                  ha=genHa(),
                  basedirpath=tdir,
                  msgHandler=beta_msg_handler.handler,
                  restricted=True)
    if start_stack:
        prepStacks(looper, beta, connect=False)

    patch_ping_pong(beta)

    return beta, beta_msg_handler


@pytest.fixture()
def create_stacks(tdir, looper, reconnect_strategy):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)

    alpha = create_alpha(tdir, looper, names[0])
    beta, beta_msg_handler = create_beta(tdir, looper, names[1],
                                         start_stack=reconnect_strategy != 'reconnect')
    return alpha, beta, beta_msg_handler


@pytest.fixture()
def send_to_disconnected(create_stacks, looper):
    # 1. create stacks (do not connect)
    alpha, beta, beta_msg_handler = create_stacks

    # 2. connect Alpha to Beta (it will be connected by TCP/CurveCP,
    # but not as remotes)
    # Beta is not connected to Alpha at all
    connectStack(alpha, beta)
    assert alpha.hasRemote(beta.name)
    assert not beta.hasRemote(alpha.name)
    assert not alpha.isConnectedTo(beta.name)
    assert not beta.isConnectedTo(alpha.name)

    # 3. send messages from Alpha to Beta
    numMsgs = 100
    msgSender = MessageSender(numMsgs=numMsgs, fromStack=alpha, toName=beta.name)
    looper.add(msgSender)

    # 4. check that all messages are sent but not received
    looper.run(eventually(msgSender.checkAllSent,
                          retryWait=1,
                          timeout=10))
    assert not alpha.isConnectedTo(beta.name)
    beta_msg_handler.check_received_from(alpha.name, 0)
    assert beta.name in alpha._stashed_to_disconnected
    assert len(alpha._stashed_to_disconnected[beta.name]) >= NUM_MSGS

    return alpha, beta, beta_msg_handler


@pytest.fixture(params=[1, 2, 3, 4, 5])
def round(request):
    pass


@pytest.fixture(params=['no_drop', 'ping_first', 'pong_first'])
def ping_pong_drop_strategy(request):
    return request.param


@pytest.fixture(params=['no_reconnect', 'reconnect'])
def reconnect_strategy(request):
    return request.param


@pytest.fixture()
def send_to_disconnected_then_connect(send_to_disconnected, looper,
                                      ping_pong_drop_strategy, reconnect_strategy,
                                      round):
    # 1. send to disconnected stack
    alpha, beta, beta_msg_handler = send_to_disconnected

    # 2. reconnect Alpha
    if reconnect_strategy == 'reconnect':
        alpha.reconnectRemoteWithName(beta.name)
        prepStacks(looper, beta, connect=False)

    # 3. Make sure that ping (pong) is the first message received by Alpha
    # dropping pong (ping)
    if ping_pong_drop_strategy == 'ping_first':
        drop_pongs(alpha)
    elif ping_pong_drop_strategy == 'pong_first':
        drop_pings(alpha)

    # 4. Connect beta to alpha
    connectStack(beta, alpha)

    # 5. make sure that ping (pong) is the first message received by Alpha
    if ping_pong_drop_strategy == 'ping_first':
        check_ping_received(looper, alpha, beta.name)
        assert not alpha.isConnectedTo(beta.name)
    elif ping_pong_drop_strategy == 'pong_first':
        check_pong_received(looper, alpha, beta.name)
        assert beta.name not in alpha.has_ping

    # 6. make sure that dropped pong (ping) is received to establish connection
    if ping_pong_drop_strategy == 'ping_first':
        re_send_pong(alpha, beta)
        check_ping_received(looper, alpha, beta.name)
        check_pong_received(looper, alpha, beta.name)
    elif ping_pong_drop_strategy == 'pong_first':
        re_send_ping(alpha, beta)
        check_ping_received(looper, alpha, beta.name)
        check_pong_received(looper, alpha, beta.name)

    return alpha, beta, beta_msg_handler


def test_send_to_disconnected_then_connect(send_to_disconnected_then_connect, looper):
    alpha, beta, beta_msg_handler = send_to_disconnected_then_connect
    check_all_received(looper, frm=alpha, to=beta, to_msg_handler=beta_msg_handler)
    assert len(alpha._stashed_to_disconnected[beta.name]) == 0
