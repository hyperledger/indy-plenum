import pytest


@pytest.mark.skip(reason="SOV-544. Not implemented")
def testNewStewardKeysWithSeeds(cli, validNodeNames, createAllNodes):
    """
    Create a CLI and issue the following command:
    new keypair with seeds A, B
    """
    cName = 'Joe'
    cli.enterCmd('new client {}'.format(cName))
    printeds = cli.printeds
    cli.looper.runFor(8)
    raise NotImplementedError


@pytest.mark.skip(reason="SOV-545. Not implemented")
def testNewStewardKeysWithoutSeeds():
    """
    Create a CLI and issue the following command:
    new keypair
    """
    raise NotImplementedError


@pytest.mark.skip(reason="SOV-546. Not implemented")
def testGeneratedKeypairIsStoredInWallet():
    """
    """
    raise NotImplementedError
