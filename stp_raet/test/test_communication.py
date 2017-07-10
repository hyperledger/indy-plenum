from ioflo.base.consoling import getConsole
from stp_core.crypto.nacl_wrappers import Signer as NaclSigner, Privateer
from raet.raeting import AutoMode, Acceptance
from raet.road.estating import RemoteEstate
from raet.road.stacking import RoadStack

from stp_raet.test.helper import handshake, sendMsgs, cleanup, getRemote
from stp_core.common.log import getlogger
from stp_core.network.port_dispenser import genHa

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
    alphaSigner = NaclSigner()
    betaSigner = NaclSigner()

    logger.debug("Alpha's verkey {}".format(alphaSigner.verhex))
    logger.debug("Beta's verkey {}".format(betaSigner.verhex))

    alpha = RoadStack(name='alpha',
                      ha=genHa(),
                      sigkey=alphaSigner.keyhex,
                      auto=AutoMode.always,
                      basedirpath=tdir)

    beta = RoadStack(name='beta',
                     ha=genHa(),
                     sigkey=betaSigner.keyhex,
                     main=True,
                     auto=AutoMode.always,
                     basedirpath=tdir)

    try:

        betaRemote = RemoteEstate(stack=alpha, ha=beta.ha,
                                  verkey=betaSigner.verhex)

        alpha.addRemote(betaRemote)

        alpha.allow(uid=betaRemote.uid, cascade=True)

        handshake(alpha, beta)

        sendMsgs(alpha, beta, betaRemote)

    finally:
        cleanup(alpha, beta)


def testRaetPreSharedKeysNonPromiscous(tdir):
    alphaSigner = NaclSigner()
    betaSigner = NaclSigner()

    alphaPrivateer = Privateer()
    betaPrivateer = Privateer()

    logger.debug("Alpha's verkey {}".format(alphaSigner.verhex))
    logger.debug("Beta's verkey {}".format(betaSigner.verhex))

    alpha = RoadStack(name='alpha',
                      ha=genHa(),
                      sigkey=alphaSigner.keyhex,
                      prikey=alphaPrivateer.keyhex,
                      auto=AutoMode.never,
                      basedirpath=tdir)

    beta = RoadStack(name='beta',
                     ha=genHa(),
                     sigkey=betaSigner.keyhex,
                     prikey=betaPrivateer.keyhex,
                     main=True,
                     auto=AutoMode.never,
                     basedirpath=tdir)

    alpha.keep.dumpRemoteRoleData({
        "acceptance": Acceptance.accepted.value,
        "verhex": betaSigner.verhex,
        "pubhex": betaPrivateer.pubhex
    }, "beta")

    beta.keep.dumpRemoteRoleData({
        "acceptance": Acceptance.accepted.value,
        "verhex": alphaSigner.verhex,
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

    alphaSigner = NaclSigner()
    betaSigner = NaclSigner()

    alphaPrivateer = Privateer()
    betaPrivateer = Privateer()

    logger.debug("Alpha's verkey {}".format(alphaSigner.verhex))
    logger.debug("Beta's verkey {}".format(betaSigner.verhex))

    alpha = None

    def setupAlpha(ha):
        nonlocal alpha
        alpha = RoadStack(name='alpha',
                          ha=ha,
                          sigkey=alphaSigner.keyhex,
                          prikey=alphaPrivateer.keyhex,
                          auto=AutoMode.never,
                          basedirpath=tdir)

        alpha.keep.dumpRemoteRoleData({
            "acceptance": Acceptance.accepted.value,
            "verhex": betaSigner.verhex,
            "pubhex": betaPrivateer.pubhex
        }, "beta")

    oldHa = genHa()
    setupAlpha(oldHa)

    beta = RoadStack(name='beta',
                     ha=genHa(),
                     sigkey=betaSigner.keyhex,
                     prikey=betaPrivateer.keyhex,
                     main=True,
                     auto=AutoMode.never,
                     basedirpath=tdir, mutable=True)

    beta.keep.dumpRemoteRoleData({
        "acceptance": Acceptance.accepted.value,
        "verhex": alphaSigner.verhex,
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
