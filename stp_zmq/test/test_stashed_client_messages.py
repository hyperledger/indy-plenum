import json

import pytest
import zmq

from plenum.common.timer import QueueTimer
from plenum.test.helper import assertExp
from stp_core.crypto.util import randomSeed
from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import SMotor
from stp_zmq.test.helper import genKeys
from stp_zmq.simple_zstack import SimpleZStack
from stp_zmq.zstack import ZStack


@pytest.fixture()
def alpha_handler(tdir, looper):
    return Handler()


@pytest.fixture()
def node_stack(tdir, looper, alpha_handler):
    name = 'Alpha'
    genKeys(tdir, name)
    aseed = randomSeed()
    stackParams = {
        "name": name,
        "ha": genHa(),
        "auto": 2,
        "basedirpath": tdir
    }
    return SimpleZStack(stackParams, lambda msg: None, aseed, False)


@pytest.fixture()
def stacks(tdir, looper, alpha_handler):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    aseed = randomSeed()
    bseed = randomSeed()

    def bHandler(m):
        msg, a = m
        beta.send(msg, a)

    stackParams = {
        "name": names[0],
        "ha": genHa(),
        "auto": 2,
        "basedirpath": tdir
    }
    alpha = SimpleZStack(stackParams, alpha_handler.handle, aseed, False)

    stackParams = {
        "name": names[1],
        "ha": genHa(),
        "auto": 2,
        "basedirpath": tdir
    }
    beta = SimpleZStack(stackParams, bHandler, bseed, True)

    amotor = SMotor(alpha)
    looper.add(amotor)

    bmotor = SMotor(beta)
    looper.add(bmotor)


class Handler:
    def __init__(self) -> None:
        self.received_messages = []

    def handle(self, m):
        d, msg = m
        self.received_messages.append(d)


def test_stash_msg_to_unknown(tdir, looper, stacks, alpha_handler):
    alpha, beta = stacks
    msg1 = {'msg': 'msg1'}
    msg2 = {'msg': 'msg2'}

    beta.send(msg1, alpha.listener.IDENTITY)
    assert beta._waiting_messages[alpha.listener.IDENTITY] == [msg1]

    alpha.connect(name=beta.name, ha=beta.ha,
                  verKeyRaw=beta.verKeyRaw, publicKeyRaw=beta.publicKeyRaw)

    looper.runFor(0.25)

    alpha.send(msg2, beta.name)
    looper.run(eventually(
        lambda msg_handler: assertExp(msg_handler.received_messages == [msg1, msg2]),
        alpha_handler))
    assert not beta._waiting_messages


def create_msg(i):
    return {'msg': 'msg{}'.format(i)}


def test_limit_msgs_for_client(tconf, looper, stacks, alpha_handler):
    alpha, beta = stacks
    for i in range(tconf.PENDING_MESSAGES_FOR_ONE_CLIENT_LIMIT + 2):
        beta.send(create_msg(i),
                  alpha.listener.IDENTITY)
    assert len(beta._waiting_messages[alpha.listener.IDENTITY]) == tconf.PENDING_MESSAGES_FOR_ONE_CLIENT_LIMIT
    assert beta._waiting_messages[alpha.listener.IDENTITY][0][1] != create_msg(0)
    assert beta._waiting_messages[alpha.listener.IDENTITY][-1][1] == create_msg(tconf.PENDING_MESSAGES_FOR_ONE_CLIENT_LIMIT + 2)


def test_limit_pending_queue(tconf, looper, stacks, alpha_handler):
    alpha, beta = stacks
    for i in range(tconf.PENDING_CLIENT_MESSAGES_LIMIT + 2):
        beta.send(create_msg(1), str(i))
    assert len(beta._waiting_messages) == tconf.PENDING_CLIENT_MESSAGES_LIMIT
    assert str(0) not in beta._waiting_messages
    assert str(tconf.PENDING_CLIENT_MESSAGES_LIMIT + 2) in beta._waiting_messages


def test_removing_old_stash(tdir, looper, stacks, node_stack):
    name = 'Alpha'
    genKeys(tdir, name)
    aseed = randomSeed()
    stackParams = {
        "name": name,
        "ha": genHa(),
        "auto": 2,
        "basedirpath": tdir
    }
    timer = QueueTimer()
    node_stack = ZStack(name,
                   ha=genHa(),
                   basedirpath=tdir,
                   msgHandler=lambda msg: None,
                   restricted=True,
                        onlyListener=True,
                        timer=QueueTimer())

    node_stack.send(create_msg(0), "id")

    assert beta._waiting_messages[alpha.listener.IDENTITY] == [msg1]

    alpha.connect(name=beta.name, ha=beta.ha,
                  verKeyRaw=beta.verKeyRaw, publicKeyRaw=beta.publicKeyRaw)

    looper.runFor(0.25)

    alpha.send(msg2, beta.name)
    looper.run(eventually(
        lambda msg_handler: assertExp(msg_handler.received_messages == [msg1, msg2]),
        alpha_handler))
    assert not beta._waiting_messages

