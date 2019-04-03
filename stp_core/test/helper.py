import json
import logging
import os

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
    assert m in [zsmsg.msg for zsmsg in p.printeds]


class CollectingMsgsHandler:
    def __init__(self):
        self.receivedMessages = []

    def handler(self, m):
        self.receivedMessages.append(m.msg)
        # print("Got message", m.msg)


class CounterMsgsHandler:
    def __init__(self):
        self.receivedMsgCount = 0
        self._received_from = {}

    def handler(self, m):
        self.receivedMsgCount += 1
        self._received_from.setdefault(m.frm, 0)
        self._received_from[m.frm] += 1

    def check_received(self, msg_num):
        assert self.receivedMsgCount == msg_num

    def check_received_from(self, sender, msg_num):
        received = self._received_from.get(sender, 0)
        assert received >= msg_num, \
            "Expected from {}: {}, but was {}. Received from others: {}" \
            .format(sender, msg_num, received, str(self._received_from))


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
                connectStack(stack, otherStack, useKeys=useKeys)


def connectStack(stack1, stack2, useKeys=True):
    stack1.connect(name=stack2.name, ha=stack2.ha,
                   verKeyRaw=stack2.verKeyRaw if useKeys else None,
                   publicKeyRaw=stack2.publicKeyRaw if useKeys else None)


def checkRemotesCreated(stacks):
    for stack in stacks:
        for otherStack in stacks:
            if stack != otherStack:
                assert stack.hasRemote(otherStack.name)


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

    def checkAllSent(self):
        assert self.sentMsgCount == self._numMsgs

    async def prod(self, limit) -> int:
        count = 0
        while self.sentMsgCount < self._numMsgs:
            msg = json.dumps({'random': randomSeed().decode()}).encode()
            if self._fromStack.send(msg, self._toName):
                self.sentMsgCount += 1
                count += 1
            else:
                break
        return count
