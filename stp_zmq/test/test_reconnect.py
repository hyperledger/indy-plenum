import time
from copy import copy

import pytest

from stp_core.loop.eventually import eventually
from stp_core.network.auth_mode import AuthMode
from stp_core.test.helper import Printer, prepStacks, \
    checkStacksConnected, checkStackDisonnected
from stp_zmq.test.helper import genKeys, add_counters_to_ping_pong
from stp_zmq.kit_zstack import KITZStack


@pytest.fixture()
def connection_timeout(tconf):
    # TODO: the connection may not be established for the first try because
    # some of the stacks may not have had a remote yet (that is they haven't had yet called connect)
    return 2 * tconf.RETRY_TIMEOUT_RESTRICTED + 1


@pytest.fixture()
def connected_stacks(registry, tdir, looper, connection_timeout):
    genKeys(tdir, registry.keys())
    stacks = []
    for name, ha in registry.items():
        printer = Printer(name)
        stackParams = dict(name=name, ha=ha, basedirpath=tdir,
                           auth_mode=AuthMode.RESTRICTED.value)
        reg = copy(registry)
        reg.pop(name)
        stack = KITZStack(stackParams, printer.print, reg)
        stacks.append(stack)

    motors = prepStacks(looper, *stacks, connect=False, useKeys=True)

    looper.run(eventually(
        checkStacksConnected, stacks, retryWait=1, timeout=connection_timeout))

    return stacks, motors


@pytest.fixture()
def disconnect_first_stack(looper, connected_stacks, connection_timeout):
    stacks, motors = connected_stacks

    disconnected_motor = motors[0]
    other_stacks = stacks[1:]

    looper.removeProdable(disconnected_motor)
    disconnected_motor.stop()

    return disconnected_motor, other_stacks


def disconnect(looper, disconnected_stack, connection_timeout):
    disconnected_motor, other_stacks = disconnected_stack
    looper.run(eventually(
        checkStackDisonnected, disconnected_motor.stack, other_stacks,
        retryWait=1, timeout=connection_timeout))
    looper.run(eventually(
        checkStacksConnected, other_stacks, retryWait=1, timeout=connection_timeout))


def connect(looper, disconnected_stack):
    disconnected_motor, _ = disconnected_stack
    looper.add(disconnected_motor)


def check_disconnected_for(disconnect_time, looper, connected_stacks,
                           connection_timeout, disconnect_first_stack):
    stacks, motors = connected_stacks

    # DISCONNECT
    disconnect(looper, disconnect_first_stack, connection_timeout)

    looper.runFor(disconnect_time)

    # CONNECT
    connect(looper, disconnect_first_stack)
    looper.run(eventually(
        checkStacksConnected, stacks, retryWait=1, timeout=2 * connection_timeout))


def test_reconnect_short(looper, connected_stacks, connection_timeout,
                         disconnect_first_stack):
    """
    Check that if a stack is kept disconnected for a short time, it is able to reconnect
    """
    check_disconnected_for(1,
                           looper, connected_stacks, connection_timeout,
                           disconnect_first_stack)


@pytest.mark.skip("Realy need more than 5 minutes????")
def test_reconnect_long(looper, connected_stacks, connection_timeout,
                        disconnect_first_stack):
    """
    Check that if a stack is kept disconnected for a long time, it is able to reconnect
    """
    check_disconnected_for(5 * 60,
                           looper, connected_stacks, connection_timeout,
                           disconnect_first_stack)


def test_recreate_sockets_after_ping_retry(looper, tconf, connected_stacks,
                                           connection_timeout,
                                           disconnect_first_stack):
    """
    Check that if a stack tries to send PING on re-connect, but not more than MAX_RECONNECT_RETRY_ON_SAME_SOCKET time.
    After this sockets must be re-created.
    """
    _, other_stacks = disconnect_first_stack
    stack = other_stacks[0]

    disconnect(looper, disconnect_first_stack, connection_timeout)

    # do not do automatic re-connect
    for stack in other_stacks:
        stack.nextCheck = time.perf_counter() + 10000000

    add_counters_to_ping_pong(stack)

    # check that sockets are not re-created MAX_RECONNECT_RETRY_ON_SAME_SOCKET times
    # and PING is called
    for i in range(tconf.MAX_RECONNECT_RETRY_ON_SAME_SOCKET):
        sockets_before = [remote.socket for name,
                          remote in stack.remotes.items()]
        ping_before = stack.sent_ping_count

        stack.retryDisconnected()

        sockets_after = [remote.socket for name,
                         remote in stack.remotes.items()]
        ping_after = stack.sent_ping_count
        assert sockets_before == sockets_after
        assert ping_before == ping_after - 1

    # check that sockets are re-created on next re-connect
    sockets_before = [remote.socket for name, remote in stack.remotes.items()]
    stack.retryDisconnected()
    sockets_after = [remote.socket for name, remote in stack.remotes.items()]
    assert sockets_before != sockets_after
