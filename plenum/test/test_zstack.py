import os
import shutil
from distutils.dir_util import copy_tree

from plenum.common.looper import Prodable
from plenum.common.motor import Motor
from plenum.common.port_dispenser import genHa
from plenum.common.z_util import generate_certificates
from plenum.common.zstack import ZStack
from plenum.test.test_node_connection import tdirAndLooper


def printer(m):
    print('Printing...')
    print(m)


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


def testRestricted2ZStackCommunication(tdirAndLooper):
    """
    Create 2 ZStack and make them send and receive messages.
    Both stacks allow communication only when keys are shared
    :return:
    """
    tdir, looper = tdirAndLooper
    names = ['Alpha', 'Beta']
    # generate_certificates(tdir, *names, clean=True)
    # for n in names:
    #     d = os.path.join(tdir, n)
    #     os.makedirs(d, exist_ok=True)
    #     for kd in ZStack.keyDirNames():
    #         copy_tree(os.path.join(tdir, kd), os.path.join(d, kd))
    genKeys(tdir, names)
    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=printer,
                   restricted=True)
    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=printer,
                  restricted=True)
    aMotor = SMotor(alpha)
    bMotor = SMotor(beta)
    looper.add(aMotor)
    looper.add(bMotor)
    looper.runFor(1)
    alpha.connect(beta.name, beta.ha, beta.verKey, beta.publicKey)
    beta.connect(alpha.name, alpha.ha, alpha.verKey, alpha.publicKey)
    looper.runFor(1.5)
    alpha.send({'greetings': 'hi'}, beta.name)
    beta.send({'greetings': 'hello'}, alpha.name)
    looper.runFor(1.5)


def testUnrestricted2ZStackCommunication(tdirAndLooper):
    """
    Create 2 ZStack and make them send and receive messages.
    Both stacks allow communication even when keys are not shared
    :return:
    """
    tdir, looper = tdirAndLooper
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=printer,
                   restricted=False)
    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=printer,
                  restricted=False)
    aMotor = SMotor(alpha)
    bMotor = SMotor(beta)
    looper.add(aMotor)
    looper.add(bMotor)
    looper.runFor(1)
    alpha.connect(beta.name, beta.ha, beta.verKey, beta.publicKey)
    beta.connect(alpha.name, alpha.ha, alpha.verKey, alpha.publicKey)
    looper.runFor(1.5)
    alpha.send({'greetings': 'hi'}, beta.name)
    beta.send({'greetings': 'hello'}, alpha.name)
    looper.runFor(1.5)
