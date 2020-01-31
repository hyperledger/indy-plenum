import random

import pytest

from stp_core.loop.eventually import eventually
from stp_core.network.auth_mode import AuthMode
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import prepStacks, connectStack, \
    checkStacksConnected, CounterMsgsHandler, MessageSender, checkStackDisonnected
from stp_zmq.kit_zstack import KITZStack
from stp_zmq.test.helper import genKeys, check_all_received

REGISTRY = {
    'Alpha': genHa(),
    'Beta': genHa()
}


@pytest.fixture()
def generated_keys(tdir):
    genKeys(tdir, REGISTRY.keys())


def create_stack(name, looper, tdir, tconf):
    msh_handler = CounterMsgsHandler()
    stackParams = dict(name=name, ha=REGISTRY[name], basedirpath=tdir,
                       auth_mode=AuthMode.RESTRICTED.value)
    stack = KITZStack(stackParams, msgHandler=msh_handler.handler, registry=REGISTRY, config=tconf)
    motor = prepStacks(looper, *[stack], connect=False, useKeys=True)[0]
    return stack, motor, msh_handler


CONNECT_TIMEOUT = 17  # this is the value we have in plenum.waits (expectedPoolInterconnectionTime) for 4 nodes


@pytest.fixture(params=range(10))
def round(request):
    pass


def send(looper, frm, to):
    num_msg = random.Random().randint(1, 100)
    msgSender = MessageSender(numMsgs=num_msg, fromStack=frm, toName=to.name)
    looper.add(msgSender)
    return num_msg


def test_connect_and_send_after_reconnect(looper, tdir, tconf, generated_keys, round):
    # create stacks
    alpha, alpha_motor, alpha_msg_handler = create_stack("Alpha", looper, tdir, tconf)
    beta, beta_motor, beta_msg_handler = create_stack("Beta", looper, tdir, tconf)

    # connect Alpha to Beta
    connectStack(alpha, beta)

    # reconnect Alpha
    alpha.reconnectRemoteWithName(beta.name)

    total = send(looper, alpha, beta)

    # connect Beta to Alpha
    connectStack(beta, alpha)

    # check connected
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], timeout=CONNECT_TIMEOUT))

    total += send(looper, alpha, beta)
    check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total)


def test_reconnect_and_send_multi(looper, tdir, tconf, generated_keys):
    # create stacks
    alpha, alpha_motor, alpha_msg_handler = create_stack("Alpha", looper, tdir, tconf)
    beta, beta_motor, beta_msg_handler = create_stack("Beta", looper, tdir, tconf)

    # connect Alpha to Beta
    connectStack(alpha, beta)
    # connect Beta to Alpha
    connectStack(beta, alpha)

    # check connected
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], timeout=CONNECT_TIMEOUT))

    total_from_alpha = 0
    total_from_alpha += send(looper, alpha, beta)
    check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total_from_alpha)

    total_from_beta = 0
    total_from_beta += send(looper, beta, alpha)
    check_all_received(looper, frm=beta, to_msg_handler=alpha_msg_handler, num_msg=total_from_beta)

    for i in range(10):
        # reconnect Alpha
        alpha.stop()

        total_from_beta += send(looper, beta, alpha)

        alpha.start()
        connectStack(alpha, beta)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], timeout=CONNECT_TIMEOUT))

        total_from_beta += send(looper, beta, alpha)
        check_all_received(looper, frm=beta, to_msg_handler=alpha_msg_handler, num_msg=total_from_beta)
        total_from_alpha += send(looper, alpha, beta)
        check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total_from_alpha)

        # reconnect Beta
        beta.stop()

        total_from_alpha += send(looper, alpha, beta)

        beta.start()
        connectStack(beta, alpha)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], timeout=CONNECT_TIMEOUT))

        total_from_alpha += send(looper, alpha, beta)
        check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total_from_alpha)

        total_from_beta += send(looper, beta, alpha)
        check_all_received(looper, frm=beta, to_msg_handler=alpha_msg_handler, num_msg=total_from_beta)

        # reconnect Alpha and then Beta
        alpha.stop()
        alpha.start()
        connectStack(alpha, beta)
        beta.stop()
        beta.start()
        connectStack(beta, alpha)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], timeout=CONNECT_TIMEOUT))

        total_from_alpha += send(looper, alpha, beta)
        check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total_from_alpha)
        total_from_beta += send(looper, beta, alpha)
        check_all_received(looper, frm=beta, to_msg_handler=alpha_msg_handler, num_msg=total_from_beta)

        # reconnect Beta and then Alpha
        beta.stop()
        beta.start()
        connectStack(beta, alpha)
        alpha.stop()
        alpha.start()
        connectStack(alpha, beta)
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], timeout=CONNECT_TIMEOUT))

        total_from_alpha += send(looper, alpha, beta)
        check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total_from_alpha)
        total_from_beta += send(looper, beta, alpha)
        check_all_received(looper, frm=beta, to_msg_handler=alpha_msg_handler, num_msg=total_from_beta)


def test_reconnect_for_long_time(looper, tdir, tconf, generated_keys):
    # create stacks
    alpha, alpha_motor, alpha_msg_handler = create_stack("Alpha", looper, tdir, tconf)
    beta, beta_motor, beta_msg_handler = create_stack("Beta", looper, tdir, tconf)

    # connect both
    connectStack(alpha, beta)
    connectStack(beta, alpha)
    looper.run(eventually(
        checkStacksConnected, [alpha, beta], timeout=CONNECT_TIMEOUT))

    total = send(looper, alpha, beta)
    check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total)

    for i in range(10):
        # 1. stop Beta
        beta.stop()
        beta_motor.stop()
        looper.run(eventually(
            checkStackDisonnected, beta, [alpha], timeout=CONNECT_TIMEOUT))
        looper.run(eventually(
            checkStackDisonnected, alpha, [beta], timeout=CONNECT_TIMEOUT))

        # 2. wait for some time
        looper.runFor(15)

        # 3. start Beta again
        beta.start()
        connectStack(beta, alpha)

        # 4. check that they are connected
        looper.run(eventually(
            checkStacksConnected, [alpha, beta], timeout=CONNECT_TIMEOUT))

        total += send(looper, alpha, beta)
        check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total)


def test_reconnect_multiple_time_with_random_waits(looper, tdir, tconf, generated_keys):
    # create stacks
    alpha, alpha_motor, alpha_msg_handler = create_stack("Alpha", looper, tdir, tconf)
    beta, beta_motor, beta_msg_handler = create_stack("Beta", looper, tdir, tconf)

    # connect both
    connectStack(alpha, beta)
    connectStack(beta, alpha)

    total = send(looper, alpha, beta)
    check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total)

    for i in range(10):
        beta.stop()

        total += send(looper, alpha, beta)

        looper.runFor(random.Random().randint(1, 10))

        beta.start()
        connectStack(beta, alpha)

        check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total)
