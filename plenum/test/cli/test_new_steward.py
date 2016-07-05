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


