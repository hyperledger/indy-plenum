import json

import zmq

from plenum.test.helper import assertExp
from stp_core.crypto.util import randomSeed
from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import SMotor
from stp_zmq.test.helper import genKeys
from stp_zmq.simple_zstack import SimpleZStack


class Handler:
    def __init__(self) -> None:
        self.received_messages = []

    def handle(self, m):
        d, msg = m
        self.received_messages.append(d)


def test_stash_msg_to_unknown(tdir, looper):
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
    handler = Handler()
    alpha = SimpleZStack(stackParams, handler.handle, aseed, False)

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
        handler))
    assert not beta._waiting_messages

