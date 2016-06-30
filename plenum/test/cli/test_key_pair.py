from plenum.common.util import getlogger

logger = getlogger()


def testKeyPair(cli, keySharedNodes):
    cli.enterCmd("new_keypair test")
    assert len(cli.defaultClient.signers) == 2
    # TODO The default client should be recovered from file.
    pubKeyMsg = cli.lastPrintArgs['msg']
    assert pubKeyMsg.startswith('Public key is')
    pubKey = pubKeyMsg.split(" ")[-1]
    logger.debug("pubKey="+pubKey)
    cli.enterCmd("list ids")
    assert cli.lastPrintArgs['msg'] == "test"
    cli.enterCmd('use_keypair {}'.format(pubKey))
    assert cli.activeSigner.verstr == pubKey
    cli.enterCmd("become test")  # TODO The same isn't working with use_keypair
    assert cli.activeSigner.verstr == pubKey
