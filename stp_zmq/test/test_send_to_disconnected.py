import types

import pytest

from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, CounterMsgsHandler, prepStacks, MessageSender, connectStack
from stp_zmq.test.helper import genKeys
from stp_zmq.zstack import ZStack

NUM_MSGS = 100


def patch_ping_pong(stack):
    origMethod = stack.handlePingPong

    stack.drop_ping = False
    stack.drop_pong = False
    stack.has_ping = False
    stack.has_pong = False

    def patchedHandlePingPong(self, msg, frm, ident):
        if self.drop_ping and msg == self.pingMessage:
            return
        if self.drop_pong and msg == self.pongMessage:
            return

        if msg == self.pingMessage:
            stack.has_ping = True
        if msg == self.pongMessage:
            stack.has_pong = True

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
    assert len(frm._send_to_disconnected[to.name]) == 0


def check_ping_received(looper, stack, frm):
    def do_check_ping():
        assert frm in stack._received_pings

    looper.run(eventually(do_check_ping))


def check_pong_received(looper, stack, frm):
    def do_check_pong():
        assert stack.hasRemote(frm) and stack.getRemote(frm).isConnected

    looper.run(eventually(do_check_pong))


def re_send_ping_pong(alpha, beta):
    alpha.drop_ping = False
    alpha.drop_pong = False

    # re-send Pong
    beta.sendPingPong(alpha.name, is_ping=False)
    # re-send Ping
    beta.sendPingPong(alpha.name, is_ping=True)


def make_sure_connected(looper, alpha, beta):
    # do re-connect as well to make sure that Alpha's re-connected socket is used
    # beta.reconnectRemoteWithName(alpha.name)
    # looper.runFor(2)
    # re_send_ping_pong(beta, alpha)
    # re_send_ping_pong(alpha, beta)
    # check_ping_received(looper, alpha, beta.name)
    # check_pong_received(looper, alpha, beta.name)
    # check_ping_received(looper, beta, alpha.name)
    # check_pong_received(looper, beta, alpha.name)
    pass


@pytest.fixture()
def create_stacks(tdir, looper):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    alphaP = Printer(names[0])
    beta_msg_handler = CounterMsgsHandler()

    alpha = ZStack(names[0],
                   ha=genHa(),
                   basedirpath=tdir,
                   msgHandler=alphaP.print,
                   restricted=True)
    beta = ZStack(names[1],
                  ha=genHa(),
                  basedirpath=tdir,
                  msgHandler=beta_msg_handler.handler,
                  restricted=True)
    prepStacks(looper, alpha, beta, connect=False)

    patch_ping_pong(alpha)
    patch_ping_pong(beta)

    return alpha, beta, beta_msg_handler


@pytest.fixture()
def send_to_disconnected(create_stacks, looper, round):
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
    assert beta.name in alpha._send_to_disconnected
    assert len(alpha._send_to_disconnected[beta.name]) >= NUM_MSGS

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
    alpha, beta, beta_msg_handler = send_to_disconnected

    if reconnect_strategy == 'reconnect':
        alpha.reconnectRemoteWithName(beta.name)
        looper.runFor(2)

    if ping_pong_drop_strategy == 'ping_first':
        drop_pongs(alpha)
    elif ping_pong_drop_strategy == 'pong_first':
        drop_pings(alpha)

    connectStack(beta, alpha)

    if ping_pong_drop_strategy == 'ping_first':
        check_ping_received(looper, alpha, beta.name)
        assert not alpha.has_pong
    elif ping_pong_drop_strategy == 'pong_first':
        check_pong_received(looper, alpha, beta.name)
        assert not alpha.has_ping

    if ping_pong_drop_strategy == 'ping_first' or ping_pong_drop_strategy == 'pong_first':
        re_send_ping_pong(alpha, beta)
        check_ping_received(looper, alpha, beta.name)
        check_pong_received(looper, alpha, beta.name)

    if reconnect_strategy == 'reconnect':
        make_sure_connected(looper, alpha, beta)

    return alpha, beta, beta_msg_handler


def test_send_to_disconnected_then_connect(send_to_disconnected_then_connect, looper):
    alpha, beta, beta_msg_handler = send_to_disconnected_then_connect
    check_all_received(looper, frm=alpha, to=beta, to_msg_handler=beta_msg_handler)
