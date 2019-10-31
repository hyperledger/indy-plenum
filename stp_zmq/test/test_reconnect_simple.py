import random

import pytest

from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import CounterMsgsHandler, Printer, prepStacks, MessageSender, connectStack
from stp_zmq.test.helper import genKeys
from stp_zmq.test.test_send_to_disconnected import check_all_received
from stp_zmq.zstack import ZStack


def create_alpha(tdir, name):
    alphaP = Printer(name)
    alpha = ZStack(name,
                   ha=genHa(),
                   basedirpath=tdir,
                   msgHandler=alphaP.print,
                   restricted=True)

    return alpha


def create_beta(tdir, name):
    beta_msg_handler = CounterMsgsHandler()
    beta = ZStack(name,
                  ha=genHa(),
                  basedirpath=tdir,
                  msgHandler=beta_msg_handler.handler,
                  restricted=True)

    return beta, beta_msg_handler


@pytest.fixture()
def create_stacks(tdir, looper):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)

    alpha = create_alpha(tdir, names[0])
    beta, beta_msg_handler = create_beta(tdir, names[1])
    return alpha, beta, beta_msg_handler


def send(looper, frm, to, num_msg):
    msgSender = MessageSender(numMsgs=num_msg, fromStack=frm, toName=to.name)
    looper.add(msgSender)


def test_reconnect_multiple_time(looper, create_stacks):
    alpha, beta, beta_msg_handler = create_stacks
    prepStacks(looper, *[alpha, beta], connect=True, useKeys=True)

    num_msg = 50
    send(looper, alpha, beta, num_msg)
    check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=num_msg)

    total = num_msg
    for i in range(10):
        beta.stop()

        num_msg = random.Random().randint(1, 100)
        total += num_msg
        send(looper, alpha, beta, num_msg)

        looper.runFor(random.Random().randint(1, 10))

        beta.start()
        connectStack(beta, alpha)

        check_all_received(looper, frm=alpha, to_msg_handler=beta_msg_handler, num_msg=total)
