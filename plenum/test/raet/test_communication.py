from raet.road.estating import RemoteEstate
from plenum.client.signer import SimpleSigner
from plenum.common.util import getlogger
from plenum.test.helper import genHa
from plenum.test.raet.helper import handshake, cleanup, sendMsgs
from raet.nacling import Privateer
from raet.raeting import AutoMode, Acceptance
from raet.road.stacking import RoadStack

logger = getlogger()


def testPromiscuousConnection(tdir):
    alpha = RoadStack(name='alpha',
                      ha=genHa(),
                      auto=AutoMode.always,
                      basedirpath=tdir)

    beta = RoadStack(name='beta',
                     ha=genHa(),
                     main=True,
                     auto=AutoMode.always,
                     basedirpath=tdir)

    try:
        betaRemote = RemoteEstate(stack=alpha, ha=beta.ha)
        alpha.addRemote(betaRemote)

        alpha.join(uid=betaRemote.uid, cascade=True)

        handshake(alpha, beta)

        sendMsgs(alpha, beta, betaRemote)
    finally:
        cleanup(alpha, beta)


def testRaetPreSharedKeysPromiscous(tdir):
    alphaSigner = SimpleSigner()
    betaSigner = SimpleSigner()

    logger.debug("Alpha's verkey {}".format(alphaSigner.verkey))
    logger.debug("Beta's verkey {}".format(betaSigner.verkey))

    alpha = RoadStack(name='alpha',
                      ha=genHa(),
                      sigkey=alphaSigner.naclSigner.keyhex,
                      auto=AutoMode.always,
                      basedirpath=tdir)

    beta = RoadStack(name='beta',
                     ha=genHa(),
                     sigkey=betaSigner.naclSigner.keyhex,
                     main=True,
                     auto=AutoMode.always,
                     basedirpath=tdir)

    try:

        betaRemote = RemoteEstate(stack=alpha, ha=beta.ha,
                                  verkey=betaSigner.verkey)

        alpha.addRemote(betaRemote)

        alpha.allow(uid=betaRemote.uid, cascade=True)

        handshake(alpha, beta)

        sendMsgs(alpha, beta, betaRemote)

    finally:
        cleanup(alpha, beta)


def testRaetPreSharedKeysNonPromiscous(tdir):
    alphaSigner = SimpleSigner()
    betaSigner = SimpleSigner()

    alphaPrivateer = Privateer()
    betaPrivateer = Privateer()

    logger.debug("Alpha's verkey {}".format(alphaSigner.verkey))
    logger.debug("Beta's verkey {}".format(betaSigner.verkey))

    alpha = RoadStack(name='alpha',
                      ha=genHa(),
                      sigkey=alphaSigner.naclSigner.keyhex,
                      prikey=alphaPrivateer.keyhex,
                      auto=AutoMode.never,
                      basedirpath=tdir)

    beta = RoadStack(name='beta',
                     ha=genHa(),
                     sigkey=betaSigner.naclSigner.keyhex,
                     prikey=betaPrivateer.keyhex,
                     main=True,
                     auto=AutoMode.never,
                     basedirpath=tdir)

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

        betaRemote = RemoteEstate(stack=alpha, ha=beta.ha)

        alpha.addRemote(betaRemote)

        alpha.allow(uid=betaRemote.uid, cascade=True)

        handshake(alpha, beta)

        sendMsgs(alpha, beta, betaRemote)
    finally:
        cleanup(alpha, beta)
