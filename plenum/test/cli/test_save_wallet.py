import pytest

from plenum.common.util import getWalletFilePath
from plenum.test.cli.helper import createAndAssertNewCreation, \
    checkWalletFilePersisted, checkPermissions, saveAndAssertKeyring


def createNewKey(do, cli, walletName):
    createAndAssertNewCreation(do, cli, walletName)


def testSaveWallet(do, be, cli):
    be(cli)
    assert cli._activeWallet is None
    createNewKey(do, cli, walletName="Default")
    saveAndAssertKeyring(do, "Default")
    filePath = getWalletFilePath(
        cli.getContextBasedWalletsBaseDir(),
        cli.walletFileName)

    checkPermissions(cli.getWalletsBaseDir(), cli.config.WALLET_DIR_MODE)
    checkPermissions(cli.getContextBasedWalletsBaseDir(),
                     cli.config.WALLET_DIR_MODE)
    checkWalletFilePersisted(filePath)
    checkPermissions(filePath, cli.config.WALLET_FILE_MODE)
