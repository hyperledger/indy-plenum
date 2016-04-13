import logging
import time
from binascii import hexlify

import raet
from raet.raeting import AutoMode, Acceptance
from raet.road.stacking import RoadStack
import raet.road.estating
from raet.nacling import Privateer
from plenum.client.signer import Signer, SimpleSigner
from plenum.common.util import getlogger, setupLogging

logger = getlogger()


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


def testRaetPreSharedKeysPromiscous():

    alphaSigner = SimpleSigner()
    betaSigner = SimpleSigner()

    logger.debug("Alpha's verkey {}".format(alphaSigner.verkey))
    logger.debug("Beta's verkey {}".format(betaSigner.verkey))

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

        alpha.addRemote(betaRemote)

        alpha.allow(uid=betaRemote.uid, cascade=True)

        handshake(alpha, beta)

        sendMsgs(alpha, beta, betaRemote)

        # alphaRemote = raet.road.estating.RemoteEstate(stack=beta,
        #                                               ha=alpha.ha,
        #                                               verkey=alphaSigner.verkey)
        #
        # beta.addRemote(alphaRemote)
        # beta.allow(uid=alphaRemote.uid, cascade=True)
        # handshake(alpha, beta)
        #
        # sendMsgs(beta, alpha, alphaRemote)

    finally:
        cleanup(alpha, beta)


def testRaetPreSharedKeysNonPromiscous():

    alphaSigner = SimpleSigner()
    betaSigner = SimpleSigner()

    alphaPrivateer = Privateer()
    betaPrivateer = Privateer()

    logger.debug("Alpha's verkey {}".format(alphaSigner.verkey))
    logger.debug("Beta's verkey {}".format(betaSigner.verkey))

    alpha = RoadStack(name='alpha',
                      ha=('0.0.0.0', 7531),
                      sigkey=alphaSigner.naclSigner.keyhex,
                      prikey=alphaPrivateer.keyhex,
                      auto=AutoMode.never)

    beta = RoadStack(name='beta',
                     ha=('0.0.0.0', 7532),
                     sigkey=betaSigner.naclSigner.keyhex,
                     prikey=betaPrivateer.keyhex,
                     main=True,
                     auto=AutoMode.never)

    alpha.keep.dumpRemoteRoleData({
        "acceptance": Acceptance.accepted.value,
        "verhex": betaSigner.verkey,
        "pubhex": betaPrivateer.pubhex
    }, "beta")

    beta.keep.dumpRemoteRoleData({
        "acceptance": Acceptance.accepted.value,
        "verhex": alphaSigner.verkey,
        "pubhex": alphaPrivateer.pubhex
    }, "alpha")

    try:

        betaRemote = raet.road.estating.RemoteEstate(stack=alpha,
                                                     ha=beta.ha)#,
                                                     # verkey=betaSigner.verkey,
                                                     # pubkey=betaPrivateer.pubhex)
        # alphaRemote = raet.road.estating.RemoteEstate(stack=beta,
        #                                               ha=alpha.ha,
        #                                               verkey=alphaSigner.verkey,
        #                                               pubkey=alphaPrivateer.pubhex)

        alpha.addRemote(betaRemote)

        alpha.allow(uid=betaRemote.uid, cascade=True)

        # beta.addRemote(alphaRemote)
        # beta.allow(uid=alphaRemote.uid, cascade=True)

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


