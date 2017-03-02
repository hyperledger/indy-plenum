import json
from copy import copy

import zmq
from plenum.common.eventually import eventually
from plenum.common.port_dispenser import genHa
from plenum.common.types import HA
from plenum.common.util import randomSeed, randomString
from plenum.common.zstack import SimpleZStack
from plenum.test.zstack_tests.helper import genKeys, Printer, prepStacks, \
    checkStacksConnected, SMotor
from raet.raeting import AutoMode


def testSimpleZStacksMsgs(tdir, looper):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    names = ['Alpha', 'Beta']
    aha = genHa()
    bha = genHa()
    aseed = randomSeed()
    bseed = randomSeed()

    size = 100000
    msg = json.dumps({'random': randomSeed(size).decode()}).encode()

    def aHandler(m):
        print('{} printing... {}'.format(names[0], m))
        d, _ = m
        print('Message size is {}'.format(len(d['random'])))
        assert len(d['random']) == size

    def bHandler(m):
        print(beta.msgHandler)
        a = list(beta.peersWithoutRemotes)[0]
        try:
            beta.listener.send_multipart([a, beta.signedMsg(msg)],
                                         flags=zmq.NOBLOCK)
        except zmq.Again:
            return False
        print('{} printing... {}'.format(names[1], m))

    stackParams = {
        "name": names[0],
        "ha": HA("0.0.0.0", aha[1]),
        "auto": AutoMode.always,
        "basedirpath": tdir
    }
    alpha = SimpleZStack(stackParams, aHandler, aseed, False)

    stackParams = {
        "name": names[1],
        "ha": HA("0.0.0.0", bha[1]),
        "auto": AutoMode.always,
        "basedirpath": tdir
    }
    beta = SimpleZStack(stackParams, bHandler, bseed, True)

    amotor = SMotor(alpha)
    looper.add(amotor)

    bmotor = SMotor(beta)
    looper.add(bmotor)

    alpha.connect(beta.name, beta.ha,
                  beta.verKey, beta.publicKey)

    looper.runFor(0.25)
    alpha.send({'greetings': 'hi'}, beta.name)
    looper.runFor(1)

