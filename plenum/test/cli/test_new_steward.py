import pytest


@pytest.mark.skipif(True, reason="Not implemented")
def testNewStewardKeysWithSeeds(cli, validNodeNames, looper, createAllNodes):
    """
    Create a CLI and issue the following command:
    new keypair with seeds A, B
    """
    cName = 'Joe'
    cli.enterCmd('new client {}'.format(cName))
    printeds = cli.printeds
    looper.runFor(8)
    assert False


@pytest.mark.skipif(True, reason="Not implemented")
def testNewStewardKeysWithoutSeeds():
    """
    Create a CLI and issue the following command:
    new keypair
    """
    raise NotImplementedError


@pytest.mark.skipif(True, reason="Not implemented")
def testGeneratedKeypairIsStoredInWallet():
    """
    """
    raise NotImplementedError


