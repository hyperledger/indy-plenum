import json

from plenum.common.port_dispenser import genHa
from plenum.common.types import HA
from plenum.common.util import randomSeed
from plenum.common.zstack import SimpleZStack
from plenum.test.zstack_tests.helper import genKeys, SMotor
from raet.raeting import AutoMode


def makeHandler(receivedMessages):
    def handler(m):
        msg, sender = m
        receivedMessages.append(msg)
        print("Got message", msg)

    return handler


def createStack(tdir, name, onlyListener, handler = lambda _: None):
    params = {
        "name": name,
        "ha": HA("0.0.0.0", genHa()[1]),
        "auto": AutoMode.always,
        "basedirpath": tdir
    }
    return SimpleZStack(params, handler, onlyListener=onlyListener)


def testMessageQuota(tdir, looper):

    names = ['Alpha', 'Beta']
    genKeys(tdir, names)

    alpha = createStack(tdir, names[0], onlyListener=False)

    receivedMessages = []
    bHandler = makeHandler(receivedMessages)
    beta = createStack(tdir, names[1], onlyListener=True, handler=bHandler)

    for stack in [alpha, beta]:
        looper.add(SMotor(stack))

    alpha.connect(beta.name, beta.ha, beta.verKey, beta.publicKey)
    looper.runFor(0.25)

    messages = []
    numMessages = 100 * beta.listenerQuota
    for i in range(numMessages):
        msg = json.dumps({'random': randomSeed().decode()}).encode()
        messages.append(json.loads(msg.decode()))
        alpha.send(msg, beta.name)

    looper.runFor(1)
    assert messages == receivedMessages
