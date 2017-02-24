import os

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


def testRestricted2ZStackCommunication(tdirAndLooper):
    """
    Create 2 ZStack and make them send and receive messages.
    Both stacks allow communication only when keys are shared
    :return:
    """
    tdir, looper = tdirAndLooper
    names = ['Alpha', 'Beta']
    for n in names:
        d = os.path.join(tdir, n)
        generate_certificates(d, *names, clean=True)
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
    looper.runFor(3)


def testUnrestricted2ZStackCommunication():
    """
    Create 2 ZStack and make them send and receive messages.
    Both stacks allow communication even when keys are not shared
    :return:
    """
