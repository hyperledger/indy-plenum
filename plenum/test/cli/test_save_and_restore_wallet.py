import pytest

from plenum.cli.cli import Exit, Cli
from plenum.common.util import normalizedWalletFileName, getWalletFilePath
from plenum.test.cli.helper import createAndAssertNewCreation, \
    createAndAssertNewKeyringCreation, useAndAssertKeyring, exitFromCli, \
    checkWalletFilePersisted, checkPermissions, saveAndAssertKeyring



def performExit(do):
    with pytest.raises(Exit):
        do('exit', within=3)


def testPersistentWalletName():
    walletFileName = normalizedWalletFileName("Default")
    assert "default.wallet" == walletFileName
    assert "default" == Cli.getWalletKeyName(walletFileName)


def createNewKey(do, cli, keyringName):
    createAndAssertNewCreation(do, cli, keyringName)


def createNewKeyring(name, do, expectedMsgs=None):
    createAndAssertNewKeyringCreation(do, name, expectedMsgs)


def useKeyring(name, do, expectedName=None, expectedMsgs=None):
    useAndAssertKeyring(do, name, expectedName, expectedMsgs)


def testSaveWallet(do, be, cli):
    be(cli)
    assert cli._activeWallet is None
    createNewKey(do, cli, keyringName="Default")
    saveAndAssertKeyring(do, "Default")
    filePath = getWalletFilePath(
        cli.getContextBasedKeyringsBaseDir(),
        cli.walletFileName)

    checkPermissions(cli.getKeyringsBaseDir(), cli.config.KEYRING_DIR_MODE)
    checkPermissions(cli.getContextBasedKeyringsBaseDir(),
                     cli.config.KEYRING_DIR_MODE)
    checkWalletFilePersisted(filePath)
    checkPermissions(filePath, cli.config.KEYRING_FILE_MODE)


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-463')
def testSaveAndRestoreWallet(do, be, cli, aliceCli):
    be(cli)
    assert cli._activeWallet is None
    createNewKey(do, cli, keyringName="Default")
    createNewKeyring("mykr0", do)
    useKeyring("Default", do)
    filePath = getWalletFilePath(
        cli.getContextBasedKeyringsBaseDir(),
        cli.walletFileName)
    exitFromCli(do)
    be(aliceCli)
    useKeyring(filePath, do, expectedName="Default")
    useKeyring("mykr0", do, expectedName="mykr0")
