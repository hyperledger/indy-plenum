from ioflo.base.consoling import getConsole
from raet.road.estating import RemoteEstate
from plenum.client.signer import SimpleSigner
from plenum.common.util import getlogger, genHa
from plenum.test.helper import genHa
from plenum.test.raet.helper import handshake, cleanup, sendMsgs, getRemote
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


def testConnectionWithHaChanged(tdir):
    console = getConsole()
    console.reinit(verbosity=console.Wordage.verbose)

    alphaSigner = SimpleSigner()
    betaSigner = SimpleSigner()

    alphaPrivateer = Privateer()
    betaPrivateer = Privateer()

    logger.debug("Alpha's verkey {}".format(alphaSigner.verkey))
    logger.debug("Beta's verkey {}".format(betaSigner.verkey))

    alpha = None

    def setupAlpha(ha):
        nonlocal alpha
        alpha = RoadStack(name='alpha',
                          ha=ha,
                          sigkey=alphaSigner.naclSigner.keyhex,
                          prikey=alphaPrivateer.keyhex,
                          auto=AutoMode.never,
                          basedirpath=tdir)

        alpha.keep.dumpRemoteRoleData({
            "acceptance": Acceptance.accepted.value,
            "verhex": betaSigner.verkey,
            "pubhex": betaPrivateer.pubhex
        }, "beta")

    oldHa = genHa()
    setupAlpha(oldHa)

    beta = RoadStack(name='beta',
                     ha=genHa(),
                     sigkey=betaSigner.naclSigner.keyhex,
                     prikey=betaPrivateer.keyhex,
                     main=True,
                     auto=AutoMode.never,
                     basedirpath=tdir, mutable=True)

    beta.keep.dumpRemoteRoleData({
        "acceptance": Acceptance.accepted.value,
        "verhex": alphaSigner.verkey,
        "pubhex": alphaPrivateer.pubhex
    }, "alpha")

    try:
        betaRemote = RemoteEstate(stack=alpha, ha=beta.ha)
        alpha.addRemote(betaRemote)
        alpha.join(uid=betaRemote.uid, cascade=True)
        handshake(alpha, beta)
        sendMsgs(alpha, beta, betaRemote)
        logger.debug("beta knows alpha as {}".
                     format(getRemote(beta, "alpha").ha))
        cleanup(alpha)

        newHa = genHa()
        logger.debug("alpha changing ha to {}".format(newHa))

        setupAlpha(newHa)
        betaRemote = RemoteEstate(stack=alpha, ha=beta.ha)
        alpha.addRemote(betaRemote)
        alpha.join(uid=betaRemote.uid, cascade=True)
        handshake(alpha, beta)
        sendMsgs(alpha, beta, betaRemote)
        logger.debug("beta knows alpha as {}".
                     format(getRemote(beta, "alpha").ha))
    finally:
        cleanup(alpha, beta)
