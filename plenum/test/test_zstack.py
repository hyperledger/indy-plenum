import os
import shutil
from distutils.dir_util import copy_tree

import asyncio
import pytest
import zmq.asyncio

from plenum.common.eventually import eventually
from plenum.common.looper import Prodable, Looper
from plenum.common.motor import Motor
from plenum.common.port_dispenser import genHa
from plenum.common.temp_file_util import SafeTemporaryDirectory
from plenum.common.z_util import generate_certificates
from plenum.common.zstack import ZStack


# Need this loop to be passed in the looper, can make a module fixture
# since need to use it with function fixture below
loop = zmq.asyncio.ZMQEventLoop()
loop.set_debug(True)


@pytest.yield_fixture()
def tdirAndLooper():
    asyncio.set_event_loop(loop)

    with SafeTemporaryDirectory() as td:
        with Looper(loop=loop, debug=True) as looper:
            yield td, looper


class Printer:
    def __init__(self):
        self.printeds = []

    def print(self, m):
        print('Printing...')
        print(m)
        self.printeds.append(m)


class SMotor(Motor):
    def __init__(self, stack):
        Motor.__init__(self)
        self.stack = stack

    async def prod(self, limit) -> int:
        return await self.stack.service(limit)

    def start(self, loop):
        self.stack.start()

    def stop(self):
        self.stack.stop()


def genKeys(directory, names):
    generate_certificates(directory, *names, clean=True)
    for n in names:
        d = os.path.join(directory, n)
        os.makedirs(d, exist_ok=True)
        for kd in ZStack.keyDirNames():
            copy_tree(os.path.join(directory, kd), os.path.join(d, kd))


def chkPrinted(p, m):
    assert m in [_[0] for _ in p.printeds]


def testRestricted2ZStackCommunication(tdirAndLooper):
    """
    Create 2 ZStack and make them send and receive messages.
    Both stacks allow communication only when keys are shared
    :return:
    """
    tdir, looper = tdirAndLooper
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    alphaP = Printer()
    betaP = Printer()

    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=alphaP.print,
                   restricted=True)
    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=betaP.print,
                  restricted=True)

    aMotor = SMotor(alpha)
    bMotor = SMotor(beta)
    looper.add(aMotor)
    looper.add(bMotor)
    looper.runFor(.5)
    alpha.connect(beta.name, beta.ha, beta.verKey, beta.publicKey)
    beta.connect(alpha.name, alpha.ha, alpha.verKey, alpha.publicKey)
    looper.runFor(.5)
    alpha.send({'greetings': 'hi'}, beta.name)
    beta.send({'greetings': 'hello'}, alpha.name)

    looper.run(eventually(chkPrinted, alphaP, {'greetings': 'hello'}))
    looper.run(eventually(chkPrinted, betaP, {'greetings': 'hi'}))


def testUnrestricted2ZStackCommunication(tdirAndLooper):
    """
    Create 2 ZStack and make them send and receive messages.
    Both stacks allow communication even when keys are not shared
    :return:
    """
    tdir, looper = tdirAndLooper
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    alphaP = Printer()
    betaP = Printer()
    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=alphaP.print,
                   restricted=False)
    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=betaP.print,
                  restricted=False)
    aMotor = SMotor(alpha)
    bMotor = SMotor(beta)
    looper.add(aMotor)
    looper.add(bMotor)
    looper.runFor(.5)
    alpha.connect(beta.name, beta.ha, beta.verKey, beta.publicKey)
    beta.connect(alpha.name, alpha.ha, alpha.verKey, alpha.publicKey)
    looper.runFor(.5)
    alpha.send({'greetings': 'hi'}, beta.name)
    beta.send({'greetings': 'hello'}, alpha.name)
    looper.runFor(.5)

    looper.run(eventually(chkPrinted, alphaP, {'greetings': 'hello'}))
    looper.run(eventually(chkPrinted, betaP, {'greetings': 'hi'}))

"""
TODO:
* Create ZKitStack, which should maintain a registry and method to check for any
disconnections and do reconnections if found.
* Need a way to run current tests against both stack types, or at least a way to
set a fixture parameter to do so.
* ZNodeStack
* ZClientStack
* test_node_connection needs to work with ZMQ
* test/pool_transactions package

"""