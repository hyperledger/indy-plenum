import types
from copy import copy

import pytest

from stp_core.loop.eventually import eventually
from stp_core.network.auth_mode import AuthMode
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, CounterMsgsHandler, prepStacks, MessageSender, connectStack, \
    checkStacksConnected, checkStackDisonnected
from stp_zmq.kit_zstack import KITZStack
from stp_zmq.test.helper import genKeys, check_pong_received
from stp_zmq.zstack import ZStack


@pytest.fixture()
def tconf(tconf):
    old_timeout = tconf.RETRY_TIMEOUT_RESTRICTED
    tconf.RETRY_TIMEOUT_RESTRICTED = 1
    # tconf.MAX_RECONNECT_RETRY_ON_SAME_SOCKET = 1000
    yield tconf
    tconf.RETRY_TIMEOUT_RESTRICTED = old_timeout


@pytest.fixture()
def registry():
    return {
        'Alpha': genHa(),
        'Beta': genHa()
    }


@pytest.fixture()
def stacks(registry, tdir, looper, tconf):
    genKeys(tdir, registry.keys())
    stacks = []
    for name, ha in registry.items():
        printer = Printer(name)
        stackParams = dict(name=name, ha=ha, basedirpath=tdir,
                           auth_mode=AuthMode.RESTRICTED.value)
        reg = copy(registry)
        reg.pop(name)
        stack = KITZStack(stackParams, msgHandler=printer.print, registry=reg, config=tconf)
        stacks.append(stack)

    prepStacks(looper, *stacks, connect=False, useKeys=True)

    return stacks

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


CONNECT_TIMEOUT = 3


def test_reconnect_multiple_times(looper, stacks):
    alpha = stacks[0]
    beta = stacks[1]

    # connect Alpha to Beta
    connectStack(alpha, beta)

    # reconnect Alpha
    alpha.reconnectRemoteWithName(beta.name)

    # connect Beta to Alpha
    connectStack(beta, alpha)

    # alpha.maintainConnections(force=True)
    # beta.maintainConnections(force=True)

    # check connected
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

    for i in range(10):
        print(i)
        # reconnect Alpha
        alpha.reconnectRemoteWithName(beta.name)
        # beta.maintainConnections(force=True)
        # alpha.maintainConnections(force=True)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

        # reconnect Beta
        beta.reconnectRemoteWithName(alpha.name)
        # alpha.maintainConnections(force=True)
        # beta.maintainConnections(force=True)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

        # reconnect Alpha and then Beta
        alpha.reconnectRemoteWithName(beta.name)
        beta.reconnectRemoteWithName(alpha.name)
        # alpha.maintainConnections(force=True)
        # beta.maintainConnections(force=True)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

        # reconnect Beta and then Alpha
        beta.reconnectRemoteWithName(alpha.name)
        alpha.reconnectRemoteWithName(beta.name)
        # alpha.maintainConnections(force=True)
        # beta.maintainConnections(force=True)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

@pytest.fixture(params=range(10))
def round(request):
    pass

def test_connect_after_reconnect(looper, stacks, round):
    alpha = stacks[0]
    beta = stacks[1]

    # connect Alpha to Beta
    connectStack(alpha, beta)

    # reconnect Alpha
    alpha.reconnectRemoteWithName(beta.name)

    # connect Beta to Alpha
    connectStack(beta, alpha)

    # alpha.maintainConnections(force=True)
    # beta.maintainConnections(force=True)

    # check connected
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))


def test_reconnect_one_multi(looper, stacks):
    alpha = stacks[0]
    beta = stacks[1]

    # connect Alpha to Beta
    connectStack(alpha, beta)
    # connect Beta to Alpha
    connectStack(beta, alpha)

    # check connected
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

    for i in range(10):
        # reconnect Alpha
        alpha.reconnectRemoteWithName(beta.name)
        # check connected
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

    # alpha.maintainConnections(force=True)
    # beta.maintainConnections(force=True)

def test_reconnect_for_long_time(looper, stacks):
    # 0. create stacks and path Beta to be able to drop pongs emulating failed attempts to connect
    alpha = stacks[0]
    beta = stacks[1]
    patch_ping_pong(beta)

    # 1. connect both
    connectStack(alpha, beta)
    connectStack(beta, alpha)
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

    for i in range(10):
        # 2. disconnect Beta
        beta.disconnectByName(alpha.name)
        looper.run(eventually(
            checkStackDisonnected, beta, [alpha], retryWait=1, timeout=CONNECT_TIMEOUT))

        # 3. wait for some time so that Alpha re-creates the socket multiple time trying to reconnect to Beta
        looper.runFor(15)

        # 4. connect Beta
        connectStack(beta, alpha)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))


