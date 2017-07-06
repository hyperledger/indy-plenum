import pytest

from plenum.common.util import getWalletFilePath
from plenum.test.cli.helper import createAndAssertNewCreation, \
    checkWalletFilePersisted, checkPermissions, saveAndAssertKeyring


def createNewKey(do, cli, keyringName):
    createAndAssertNewCreation(do, cli, keyringName)


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
