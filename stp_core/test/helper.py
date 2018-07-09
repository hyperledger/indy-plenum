import os

import logging

import json
from stp_core.crypto.util import randomSeed

from stp_core.loop.motor import Motor
from stp_core.network.keep_in_touch import KITNetworkInterface

logger = logging.getLogger()


def createTempDir(tmpdir_factory, counter):
    tempdir = os.path.join(tmpdir_factory.getbasetemp().strpath,
                           str(next(counter)))
    logger.debug("module-level temporary directory: {}".format(tempdir))
    return tempdir


class Printer:
    def __init__(self, name):
        self.name = name
        self.printeds = []

    def print(self, m):
        str_m = "{}".format(m)
        print('{} printing... {}'.format(self.name, str_m[:100]))
        self.printeds.append(m)

    def reset(self):
        self.printeds = []


def chkPrinted(p, m):
    assert m in [_[0] for _ in p.printeds]


class CollectingMsgsHandler:
    def __init__(self):
        self.receivedMessages = []

    def handler(self, m):
        msg, sender = m
        self.receivedMessages.append(msg)
        # print("Got message", msg)


class CounterMsgsHandler:
    def __init__(self):
        self.receivedMsgCount = 0

    def handler(self, m):
        msg, sender = m
        self.receivedMsgCount += 1


class SMotor(Motor):
    def __init__(self, stack):
        Motor.__init__(self)
        self.stack = stack

    async def prod(self, limit) -> int:
        c = await self.stack.service(limit)
        if isinstance(self.stack, KITNetworkInterface):
            self.stack.serviceLifecycle()
        return c

    def start(self, loop):
        self.stack.start()

    def stop(self):
        self.stack.stop()


def prepStacks(looper, *stacks, connect=True, useKeys=True):
    motors = []
    for stack in stacks:
        motor = SMotor(stack)
        looper.add(motor)
        motors.append(motor)
    if connect:
        connectStacks(stacks, useKeys)
        looper.runFor(1)
    return motors


def connectStacks(stacks, useKeys=True):
    for stack in stacks:
        for otherStack in stacks:
            if stack != otherStack:
                stack.connect(name=otherStack.name, ha=otherStack.ha,
                              verKeyRaw=otherStack.verKeyRaw if useKeys else None,
                              publicKeyRaw=otherStack.publicKeyRaw if useKeys else None)


def checkStacksConnected(stacks):
    for stack in stacks:
        for otherStack in stacks:
            if stack != otherStack:
                assert stack.isConnectedTo(otherStack.name)


def checkStackConnected(stack, stacks):
    for other in stacks:
        assert stack.isConnectedTo(other.name)


def checkStackDisonnected(stack, stacks):
    for other in stacks:
        assert not stack.isConnectedTo(other.name)


class MessageSender(Motor):
    def __init__(self, numMsgs, fromStack, toName):
        super().__init__()
        self._numMsgs = numMsgs
        self._fromStack = fromStack
        self._toName = toName
        self.sentMsgCount = 0

    def _statusChanged(self, old, new):
        pass

    def onStopping(self, *args, **kwargs):
        pass

    async def prod(self, limit) -> int:
        count = 0
        while self.sentMsgCount < self._numMsgs:
            msg = json.dumps({'random': randomSeed().decode()}).encode()
            self._fromStack.send(msg, self._toName)
            self.sentMsgCount += 1
            count += 1
        return count
