import pytest
from plenum.cli.cli import Exit, Cli
from plenum.test.cli.helper import createAndAssertNewCreation, \
    createAndAssertNewKeyringCreation, useAndAssertKeyring, exitFromCli


def performExit(do):
    with pytest.raises(Exit):
        do('exit', within=3)


def testPersistentWalletName():
    walletFileName = Cli._normalizedWalletFileName("Default")
    assert "default.wallet" == walletFileName
    assert "default" == Cli.getWalletKeyName(walletFileName)


def createNewKey(do, cli, keyringName):
    createAndAssertNewCreation(do, cli, keyringName)


def createNewKeyring(name, do, expectedMsgs=None):
    createAndAssertNewKeyringCreation(do, name, expectedMsgs)


def useKeyring(name, do, expectedName=None, expectedMsgs=None):
    useAndAssertKeyring(do, name, expectedName, expectedMsgs)


def testSaveAndRestoreWallet(do, be, cli, aliceCli):
    be(cli)
    assert cli._activeWallet is None
    createNewKey(do, cli, keyringName="Default")
    createNewKeyring("mykr0", do)
    useKeyring("Default", do)
    filePath = Cli.getWalletFilePath(
        cli.getContextBasedKeyringsBaseDir(),
        cli.walletFileName)
    exitFromCli(do)
    be(aliceCli)
    useKeyring(filePath, do, expectedName="Default")
    useKeyring("mykr0", do, expectedName="mykr0")