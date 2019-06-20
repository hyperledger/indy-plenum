import json

import zmq
from stp_core.crypto.util import randomSeed
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import SMotor
from stp_zmq.test.helper import genKeys
from stp_zmq.simple_zstack import SimpleZStack


def testSimpleZStacksMsgs(tdir, looper):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    names = ['Alpha', 'Beta']
    aseed = randomSeed()
    bseed = randomSeed()

    size = 100000
    msg = json.dumps({'random': randomSeed(size).decode()}).encode()

    def aHandler(m):
        str_m = "{}".format(m)
        print('{} printing... {}'.format(names[0], str_m[:100]))
        d, _ = m
        print('Message size is {}'.format(len(d['random'])))
        assert len(d['random']) == size

    def bHandler(m):
        print(beta.msgHandler)
        a = m[1]
        try:
            beta.listener.send_multipart([a, msg],
                                         flags=zmq.NOBLOCK)
        except zmq.Again:
            return False
        str_m = "{}".format(m)
        print('{} printing... {}'.format(names[1], str_m[:100]))

    stackParams = {
        "name": names[0],
        "ha": genHa(),
        "auto": 2,
        "basedirpath": tdir
    }
    alpha = SimpleZStack(stackParams, aHandler, aseed, False)

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

    alpha.connect(name=beta.name, ha=beta.ha,
                  verKeyRaw=beta.verKeyRaw, publicKeyRaw=beta.publicKeyRaw)

    looper.runFor(0.25)
    alpha.send({'greetings': 'hi'}, beta.name)
    looper.runFor(1)
