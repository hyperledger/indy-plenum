import os
from distutils.dir_util import copy_tree

from plenum.common.z_util import generate_certificates
from plenum.common.zstack import ZStack, KITZStack

from plenum.common.motor import Motor


class Printer:
    def __init__(self, name):
        self.name = name
        self.printeds = []

    def print(self, m):
        print('{} printing... {}'.format(self.name, m))
        self.printeds.append(m)


class SMotor(Motor):
    def __init__(self, stack):
        Motor.__init__(self)
        self.stack = stack

    async def prod(self, limit) -> int:
        c = await self.stack.service(limit)
        if isinstance(self.stack, KITZStack):
            self.stack.serviceLifecycle()
        return c

    def start(self, loop):
        self.stack.start()

    def stop(self):
        self.stack.stop()


def genKeys(baseDir, names):
    generate_certificates(baseDir, *names, clean=True)
    for n in names:
        d = os.path.join(baseDir, n)
        os.makedirs(d, exist_ok=True)
        for kd in ZStack.keyDirNames():
            copy_tree(os.path.join(baseDir, kd), os.path.join(d, kd))


def chkPrinted(p, m):
    assert m in [_[0] for _ in p.printeds]


def prepStacks(looper, *stacks, connect=True):
    for stack in stacks:
        motor = SMotor(stack)
        looper.add(motor)
    if connect:
        connectStacks(stacks)


def connectStacks(stacks):
    for stack in stacks:
        for otherStack in stacks:
            if stack != otherStack:
                stack.connect(otherStack.name, otherStack.ha,
                              otherStack.verKey, otherStack.publicKey)


def checkStacksConnected(stacks):
    for stack in stacks:
        for otherStack in stacks:
            if stack != otherStack:
                assert otherStack.name in stack.connecteds
                assert stack.name in otherStack.connecteds
