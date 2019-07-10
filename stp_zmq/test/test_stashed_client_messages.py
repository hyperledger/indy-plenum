import os
import time

import pytest
import zmq
from plenum.common.util import randomString, randomSeed

from stp_core.common.constants import ZMQ_NETWORK_PROTOCOL
from stp_core.loop.eventually import eventually
from stp_core.network.auth_mode import AuthMode
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, prepStacks, CounterMsgsHandler, connectStack
from stp_core.types import HA
from stp_zmq.remote import set_keepalive, set_zmq_internal_queue_size
from stp_zmq.simple_zstack import SimpleZStack
from stp_zmq.test.helper import genKeys, check_pong_received
from stp_zmq.zstack import ZStack


@pytest.fixture()
def stacks(tdir, looper):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    return create_zstack(names[0], tdir, looper), create_zstack(names[0], tdir, looper)


def create_zstack(name, tdir, looper, listener=False):
    alphaP = Printer(name)
    alpha = ZStack(name,
           ha=genHa(),
           basedirpath=tdir,
           msgHandler=alphaP.print,
           restricted=True)

    prepStacks(looper, alpha, connect=False)
    return alpha, alphaP


def test_stash_msg_to_unknown(looper, stacks):
    '''
    Beta should stash the pong for Alpha on Alpha's ping received when
    Alpha is not connected yet.
    '''
    (alpha, _), (beta, betaP) = stacks
    alpha.transmitThroughListener('"msg1"', beta.name)
    alpha.connect(name=beta.name, ha=beta.ha,
                  verKeyRaw=beta.verKeyRaw, publicKeyRaw=beta.publicKeyRaw)
    looper.runFor(0.25)
    # prepStacks(looper, alpha, beta, connect=True, useKeys=True)
    alpha.transmitThroughListener('"msg2"', beta.name)
    assert not alpha._waiting_messages
    looper.runFor(20)
    print(betaP.printeds)
    # assert betaP.printeds == [msg1, msg2]
    # # msg = beta.rxMsgs.popleft()
    # looper.runFor(20)
    # msg1 = (betaP.rxMsgs.popleft())
    # msg2 = print(beta.rxMsgs.popleft())
    # print(1)

    # check_stashed_pongs(looper, stack=beta, to=alpha.publicKey)

    # connectStack(beta, alpha)
    # check_no_stashed_pongs(looper, stack=beta, to=alpha.publicKey)
    #
    # check_pong_received(looper, alpha, beta.name)
