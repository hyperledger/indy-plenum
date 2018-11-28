from binascii import unhexlify

from stp_core.crypto.util import ed25519SkToCurve25519, ed25519PkToCurve25519


def testNodesConnectedUsingConvertedKeys(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        secretKey = ed25519SkToCurve25519(node.nodestack.keyhex)
        publicKey = ed25519PkToCurve25519(node.nodestack.verhex)
        assert unhexlify(node.nodestack.prihex) == secretKey
        assert unhexlify(node.nodestack.pubhex) == publicKey

        secretKey = ed25519SkToCurve25519(node.clientstack.keyhex)
        publicKey = ed25519PkToCurve25519(node.clientstack.verhex)
        assert unhexlify(node.clientstack.prihex) == secretKey
        assert unhexlify(node.clientstack.pubhex) == publicKey
