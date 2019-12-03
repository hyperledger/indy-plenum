import types

import pytest

from stp_core.loop.eventually import eventually
from stp_core.network.auth_mode import AuthMode
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, prepStacks, connectStack, \
    checkStacksConnected, checkStackDisonnected, checkStackConnected
from stp_zmq.kit_zstack import KITZStack
from stp_zmq.test.helper import genKeys


@pytest.fixture()
def tconf(tconf):
    old_timeout = tconf.RETRY_TIMEOUT_RESTRICTED
    tconf.RETRY_TIMEOUT_RESTRICTED = 1
    yield tconf
    tconf.RETRY_TIMEOUT_RESTRICTED = old_timeout


REGISTRY = {
    'Alpha': genHa(),
    'Beta': genHa()
}


@pytest.fixture()
def generated_keys(tdir):
    genKeys(tdir, REGISTRY.keys())


def create_stack(name, looper, tdir, tconf):
    printer = Printer(name)
    stackParams = dict(name=name, ha=REGISTRY[name], basedirpath=tdir,
                       auth_mode=AuthMode.RESTRICTED.value)
    stack = KITZStack(stackParams, msgHandler=printer.print, registry=REGISTRY, config=tconf)
    patch_ping_pong(stack)
    motor = prepStacks(looper, *[stack], connect=False, useKeys=True)[0]
    return stack, motor


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


CONNECT_TIMEOUT = 17  # this is the value we have in plenum.waits (expectedPoolInterconnectionTime) for 4 nodes


@pytest.fixture(params=range(50))
def round(request):
    pass


def test_connect_after_reconnect(looper, tdir, tconf, generated_keys, round):
    # create stacks
    alpha, alpha_motor = create_stack("Alpha", looper, tdir, tconf)
    beta, beta_motor = create_stack("Beta", looper, tdir, tconf)

    # connect Alpha to Beta
    connectStack(alpha, beta)

    # reconnect Alpha
    alpha.reconnectRemoteWithName(beta.name)

    # connect Beta to Alpha
    connectStack(beta, alpha)

    # check connected
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))


def test_reconnect_one_multi(looper, tdir, tconf, generated_keys):
    # create stacks
    alpha, alpha_motor = create_stack("Alpha", looper, tdir, tconf)
    beta, beta_motor = create_stack("Beta", looper, tdir, tconf)

    # connect Alpha to Beta
    connectStack(alpha, beta)
    # connect Beta to Alpha
    connectStack(beta, alpha)

    # check connected
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

    for i in range(20):
        # reconnect Alpha
        alpha.reconnectRemoteWithName(beta.name)
        # check connected
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))


def test_reconnect_multiple_times(looper, tdir, tconf, generated_keys):
    # create stacks
    alpha, alpha_motor = create_stack("Alpha", looper, tdir, tconf)
    beta, beta_motor = create_stack("Beta", looper, tdir, tconf)

    # connect Alpha to Beta
    connectStack(alpha, beta)

    # reconnect Alpha
    alpha.reconnectRemoteWithName(beta.name)

    # connect Beta to Alpha
    connectStack(beta, alpha)

    # check connected
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

    for i in range(10):
        print(i)
        # reconnect Alpha
        alpha.reconnectRemoteWithName(beta.name)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

        # reconnect Beta
        beta.reconnectRemoteWithName(alpha.name)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

        # reconnect Alpha and then Beta
        alpha.reconnectRemoteWithName(beta.name)
        beta.reconnectRemoteWithName(alpha.name)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

        # reconnect Beta and then Alpha
        beta.reconnectRemoteWithName(alpha.name)
        alpha.reconnectRemoteWithName(beta.name)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))


def test_reconnect_for_long_time(looper, tdir, tconf, generated_keys):
    # create stacks
    alpha, alpha_motor = create_stack("Alpha", looper, tdir, tconf)
    beta, beta_motor = create_stack("Beta", looper, tdir, tconf)

    # connect both
    connectStack(alpha, beta)
    connectStack(beta, alpha)
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

    for i in range(10):
        # 1. stop Beta
        looper.removeProdable(beta_motor)
        beta_motor.stop()
        looper.run(eventually(
            checkStackDisonnected, beta, [alpha], retryWait=1, timeout=CONNECT_TIMEOUT))
        looper.run(eventually(
            checkStackDisonnected, alpha, [beta], retryWait=1, timeout=CONNECT_TIMEOUT))

        # 2. wait for some time so that Alpha re-creates the socket multiple time trying to reconnect to Beta
        looper.runFor(15)

        # 3. start Beta again
        beta, beta_motor = create_stack("Beta", looper, tdir, tconf)
        connectStack(beta, alpha)

        # 4. check that they are connected
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))


@pytest.mark.skip(reason='INDY-2289: if zeroMQ auto reconnect works some of the use cases do not make sense anymore')
def test_reconnect_for_long_time_lose_pongs(looper, tdir, tconf, generated_keys):
    # create stacks
    alpha, alpha_motor = create_stack("Alpha", looper, tdir, tconf)
    beta, beta_motor = create_stack("Beta", looper, tdir, tconf)

    # connect both
    connectStack(alpha, beta)
    connectStack(beta, alpha)
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))

    for i in range(10):
        # 1. stop Beta
        looper.removeProdable(beta_motor)
        beta_motor.stop()
        looper.run(eventually(
            checkStackDisonnected, beta, [alpha], retryWait=1, timeout=CONNECT_TIMEOUT))
        looper.run(eventually(
            checkStackDisonnected, alpha, [beta], retryWait=1, timeout=CONNECT_TIMEOUT))

        # 2. wait for some time so that Alpha re-creates the socket multiple time trying to reconnect to Beta
        looper.runFor(15)

        # 3. start Beta again but drop pongs to emulate a situation when Alpha connected to Beta, but Beta doesn't
        beta, beta_motor = create_stack("Beta", looper, tdir, tconf)
        beta.drop_pong = True
        connectStack(beta, alpha)

        # 4. make sure that only Alpha is connected
        looper.run(eventually(
            checkStackConnected, alpha, [beta], retryWait=1, timeout=CONNECT_TIMEOUT))
        looper.run(eventually(
            checkStackDisonnected, beta, [alpha], retryWait=1, timeout=CONNECT_TIMEOUT))

        # 5. allow beta to connect to Alpha
        beta.drop_pong = False

        # 6. check that they both connected
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], retryWait=1, timeout=CONNECT_TIMEOUT))
