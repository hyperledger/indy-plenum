import time
from binascii import hexlify

import raet
from raet.raeting import AutoMode
from raet.road.stacking import RoadStack

from plenum.client.signer import Signer, SimpleSigner


def testPromiscuousConnection():
    alpha = RoadStack(name='alpha',
                      ha=('0.0.0.0', 7531),
                      auto=AutoMode.always)

    beta = RoadStack(name='beta',
                     ha=('0.0.0.0', 7532),
                     main=True,
                     auto=AutoMode.always)

    try:
        betaRemote = raet.road.estating.RemoteEstate(stack=alpha,
                                                     ha=beta.ha)
        alpha.addRemote(betaRemote)

        alpha.join(uid=betaRemote.uid, cascade=True)

        handshake(alpha, beta)

        sendMsgs(alpha, beta, betaRemote)
    finally:
        cleanup(alpha, beta)


def testRaetPreSharedKeys():

    alphaSigner = SimpleSigner()
    betaSigner = SimpleSigner()

    alpha = RoadStack(name='alpha',
                      ha=('0.0.0.0', 7531),
                      sigkey=alphaSigner.naclSigner.keyhex,
                      auto=AutoMode.always)

    beta = RoadStack(name='beta',
                     ha=('0.0.0.0', 7532),
                     sigkey=betaSigner.naclSigner.keyhex,
                     main=True,
                     auto=AutoMode.always)

    try:

        betaRemote = raet.road.estating.RemoteEstate(stack=alpha,
                                                     ha=beta.ha,
                                                     verkey=betaSigner.verkey)
        alphaRemote = raet.road.estating.RemoteEstate(stack=beta,
                                                      ha=alpha.ha,
                                                      verkey=alphaSigner.verkey)
        alpha.addRemote(betaRemote)

        alpha.allow(uid=betaRemote.uid, cascade=True)

        handshake(alpha, beta)

        sendMsgs(alpha, beta, betaRemote)
    finally:
        cleanup(alpha, beta)


def handshake(*stacks):
    svc(stacks)
    print("Finished Handshake\n")


def svc(stacks):
    while True:
        for stack in stacks:
            stack.serviceAll()
            stack.store.advanceStamp(0.1)
        if all([not stack.transactions for stack in stacks]):
            break
        time.sleep(0.1)


def sendMsgs(alpha, beta, betaRemote):
    stacks = [alpha, beta]
    msg = {'subject': 'Example message alpha to beta',
           'content': 'test'}
    alpha.transmit(msg, betaRemote.uid)
    svc(stacks)
    rx = beta.rxMsgs.popleft()
    print("{0}\n".format(rx))
    print("Finished Message alpha to beta\n")
    msg = {'subject': 'Example message beta to alpha',
           'content': 'Another test.'}
    beta.transmit(msg, betaRemote.uid)
    svc(stacks)
    rx = alpha.rxMsgs.popleft()
    print("{0}\n".format(rx))
    print("Finished Message beta to alpha\n")


def cleanup(*stacks):
    for stack in stacks:
        stack.server.close()  # close the UDP socket
        stack.keep.clearAllDir()  # clear persisted data
    print("Finished\n")


