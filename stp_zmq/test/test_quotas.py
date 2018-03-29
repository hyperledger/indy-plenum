import json

import pytest
from stp_core.loop.eventually import eventually
from stp_core.crypto.util import randomSeed
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, prepStacks, CollectingMsgsHandler, CounterMsgsHandler, MessageSender
from stp_zmq.test.helper import genKeys
from stp_zmq.zstack import ZStack
from stp_zmq.test.conftest import BIG_NUM_OF_MSGS


def testMessageQuota(set_info_log_level, tdir, looper):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    alphaP = Printer(names[0])
    betaMsgHandler = CollectingMsgsHandler()

    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=alphaP.print,
                   restricted=True)
    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=betaMsgHandler.handler,
                  restricted=True, onlyListener=True)

    prepStacks(looper, alpha, beta, connect=True, useKeys=True)

    messages = []
    numMessages = 150 * beta.listenerQuota
    for i in range(numMessages):
        msg = json.dumps({'random': randomSeed().decode()}).encode()
        if alpha.send(msg, beta.name):
            messages.append(json.loads(msg.decode()))

    def checkAllReceived():
        assert len(messages) == len(betaMsgHandler.receivedMessages)
        assert messages == betaMsgHandler.receivedMessages

    looper.run(eventually(checkAllReceived, retryWait=0.5,
                          timeout=5))


def testManyMessages(set_info_log_level, tdir, looper, tconf):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    alphaP = Printer(names[0])
    betaMsgHandler = CounterMsgsHandler()

    alpha = ZStack(names[0],
                   ha=genHa(),
                   basedirpath=tdir,
                   msgHandler=alphaP.print,
                   restricted=True)
    beta = ZStack(names[1],
                  ha=genHa(),
                  basedirpath=tdir,
                  msgHandler=betaMsgHandler.handler,
                  restricted=True)
    prepStacks(looper, alpha, beta, connect=True, useKeys=True)

    looper.runFor(1)

    msgNum = BIG_NUM_OF_MSGS
    msgSender = MessageSender(msgNum, alpha, beta.name)
    looper.add(msgSender)

    def checkAllReceived():
        assert msgSender.sentMsgCount == msgNum
        assert betaMsgHandler.receivedMsgCount == msgNum

    looper.run(eventually(checkAllReceived,
                          retryWait=1,
                          timeout=60))
