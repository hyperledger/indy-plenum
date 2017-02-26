from binascii import unhexlify

from plenum.common.crypto import ed25519SkToCurve25519, ed25519PkToCurve25519


def testNodesConnectedUsingConvertedKeys(nodeSet, up):
    for node in nodeSet:
        secretKey = ed25519SkToCurve25519(unhexlify(node.nodestack.keyhex))
        publicKey = ed25519PkToCurve25519(unhexlify(node.nodestack.verhex))
        assert unhexlify(node.nodestack.prihex) == secretKey
        assert unhexlify(node.nodestack.pubhex) == publicKey

        secretKey = ed25519SkToCurve25519(unhexlify(node.clientstack.keyhex))
        publicKey = ed25519PkToCurve25519(unhexlify(node.clientstack.verhex))
        assert unhexlify(node.clientstack.prihex) == secretKey
        assert unhexlify(node.clientstack.pubhex) == publicKey


def testClientConnectedUsingConvertedKeys(nodeSet, up, client1, replied1):
    secretKey = ed25519SkToCurve25519(unhexlify(client1.nodestack.keyhex))
    publicKey = ed25519PkToCurve25519(unhexlify(client1.nodestack.verhex))
    assert unhexlify(client1.nodestack.prihex) == secretKey
    assert unhexlify(client1.nodestack.pubhex) == publicKey
