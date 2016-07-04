def testNewStewardKeysWithSeeds(cli, validNodeNames, cliLooper, createAllNodes):
    """
    Create a CLI and issue the following command:
    new keypair with seeds A, B
    """
    cName = 'Joe'
    cli.enterCmd('new client {}'.format(cName))
    printeds = cli.printeds
    cliLooper.runFor(8)
    assert False


def testNewStewardKeysWithoutSeeds():
    """
    Create a CLI and issue the following command:
    new keypair
    """
    raise NotImplementedError


def testGeneratedKeypairIsStoredInWallet():
    """
    """
    raise NotImplementedError


