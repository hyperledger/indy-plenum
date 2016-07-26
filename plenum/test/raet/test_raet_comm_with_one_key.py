from binascii import hexlify

from plenum.common.signing import ed25519SkToCurve25519, ed25519PkToCurve25519
from raet.nacling import Signer
from raet.road.estating import RemoteEstate
from plenum.test.helper import genHa
from plenum.test.raet.helper import handshake, cleanup, sendMsgs
from raet.raeting import AutoMode, Acceptance
from raet.road.stacking import RoadStack
from libnacl import nacl


def testNonPromiscousConnectionWithOneKey(tdir):
    # Simulating node to node connection
    alphaSigner = Signer()
    betaSigner = Signer()
    alphaPrikey = ed25519SkToCurve25519(alphaSigner.keyraw)
    betaPrikey = ed25519SkToCurve25519(betaSigner.keyraw)
    alphaPubkey = ed25519PkToCurve25519(alphaSigner.verraw)
    betaPubkey = ed25519PkToCurve25519(betaSigner.verraw)
    alphaName = 'alpha'
    betaName = 'beta'

    alpha = RoadStack(name=alphaName,
                      ha=genHa(),
                      sigkey=alphaSigner.keyhex,
                      prikey=hexlify(alphaPrikey),
                      auto=AutoMode.never,
                      basedirpath=tdir)

    beta = RoadStack(name=betaName,
                     ha=genHa(),
                     sigkey=betaSigner.keyhex,
                     prikey=hexlify(betaPrikey),
                     main=True,
                     auto=AutoMode.never,
                     basedirpath=tdir)

    alpha.keep.dumpRemoteRoleData({
        "acceptance": Acceptance.accepted.value,
        "verhex": betaSigner.verhex,
        "pubhex": hexlify(betaPubkey)
    }, betaName)

    beta.keep.dumpRemoteRoleData({
        "acceptance": Acceptance.accepted.value,
        "verhex": alphaSigner.verhex,
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
