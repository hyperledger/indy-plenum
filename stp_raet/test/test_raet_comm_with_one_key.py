from binascii import hexlify

import pytest
from raet.raeting import AutoMode, Acceptance
from raet.road.estating import RemoteEstate
from raet.road.stacking import RoadStack

from stp_raet.test.helper import handshake, sendMsgs, cleanup
from stp_core.crypto.nacl_wrappers import Signer
from stp_core.crypto.util import ed25519SkToCurve25519, ed25519PkToCurve25519
from stp_core.network.port_dispenser import genHa


@pytest.fixture(scope="module")
def keysAndNames():
    alphaSigner = Signer()
    betaSigner = Signer()
    alphaPrikey = ed25519SkToCurve25519(alphaSigner.keyraw)
    betaPrikey = ed25519SkToCurve25519(betaSigner.keyraw)
    alphaPubkey = ed25519PkToCurve25519(alphaSigner.verraw)
    betaPubkey = ed25519PkToCurve25519(betaSigner.verraw)
    alphaName = 'alpha'
    betaName = 'beta'
    return alphaSigner.keyhex, alphaPrikey, alphaSigner.verhex, alphaPubkey, \
        alphaName, betaSigner.keyhex, betaPrikey, betaSigner.verhex, \
        betaPubkey, betaName


def testNonPromiscousConnectionWithOneKey(tdir, keysAndNames):
    # Simulating node to node connection
    alphaSighex, alphaPrikey, alphaVerhex, alphaPubkey, alphaName, betaSighex,\
        betaPrikey, betaVerhex, betaPubkey, betaName = keysAndNames
    alpha = RoadStack(name=alphaName,
                      ha=genHa(),
                      sigkey=alphaSighex,
                      prikey=hexlify(alphaPrikey),
                      auto=AutoMode.never,
                      basedirpath=tdir)

    beta = RoadStack(name=betaName,
                     ha=genHa(),
                     sigkey=betaSighex,
                     prikey=hexlify(betaPrikey),
                     main=True,
                     auto=AutoMode.never,
                     basedirpath=tdir)

    alpha.keep.dumpRemoteRoleData({
        "acceptance": Acceptance.accepted.value,
        "verhex": betaVerhex,
        "pubhex": hexlify(betaPubkey)
    }, betaName)

    beta.keep.dumpRemoteRoleData({
        "acceptance": Acceptance.accepted.value,
        "verhex": alphaVerhex,
        "pubhex": hexlify(alphaPubkey)
    }, alphaName)

    try:

        betaRemote = RemoteEstate(stack=alpha, ha=beta.ha)

        alpha.addRemote(betaRemote)

        alpha.allow(uid=betaRemote.uid, cascade=True)

        handshake(alpha, beta)

        sendMsgs(alpha, beta, betaRemote)
    finally:
        cleanup(alpha, beta)


def testPromiscuousConnection(tdir, keysAndNames):
    # Simulating node to client connection
    alphaSighex, alphaPrikey, alphaVerhex, alphaPubkey, alphaName, betaSighex, \
        betaPrikey, betaVerhex, betaPubkey, betaName = keysAndNames
    alpha = RoadStack(name=alphaName,
                      ha=genHa(),
                      sigkey=alphaSighex,
                      prikey=hexlify(alphaPrikey),
                      auto=AutoMode.always,
                      basedirpath=tdir)

    beta = RoadStack(name=betaName,
                     ha=genHa(),
                     main=True,
                     sigkey=betaSighex,
                     prikey=hexlify(betaPrikey),
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
