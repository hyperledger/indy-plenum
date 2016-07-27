from plenum.common.crypto import ed25519SkToCurve25519, ed25519PkToCurve25519


def testNodesConnectedUsingConvertedKeys(nodeSet, up):
    for node in nodeSet:
        secretKey = ed25519SkToCurve25519(node.nodestack.local.signer.keyraw)
        publicKey = ed25519PkToCurve25519(node.nodestack.local.signer.verraw)
        assert node.nodestack.local.priver.keyraw == secretKey
        assert node.nodestack.local.priver.pubraw == publicKey

        secretKey = ed25519SkToCurve25519(node.clientstack.local.signer.keyraw)
        publicKey = ed25519PkToCurve25519(node.clientstack.local.signer.verraw)
        assert node.clientstack.local.priver.keyraw == secretKey
        assert node.clientstack.local.priver.pubraw == publicKey


def testClientConnectedUsingConvertedKeys(nodeSet, up, client1, replied1):
    secretKey = ed25519SkToCurve25519(client1.nodestack.local.signer.keyraw)
    publicKey = ed25519PkToCurve25519(client1.nodestack.local.signer.verraw)
    assert client1.nodestack.local.priver.keyraw == secretKey
    assert client1.nodestack.local.priver.pubraw == publicKey
