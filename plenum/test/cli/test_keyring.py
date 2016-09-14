def createNewKeyring(name, cli):
    oldKeyring = cli._activeWallet
    cli.enterCmd("new keyring {}".format(name))
    assert 'Active wallet set to "{}"'.format(name) in cli.lastCmdOutput
    assert 'New wallet {} created'.format(name) in cli.lastCmdOutput
    assert not oldKeyring or (
    oldKeyring and oldKeyring.name != cli._activeWallet.name)
    assert cli.activeWallet.name == name
    assert len(cli._activeWallet.signers) == 0


def testNewKeyring(cli):
    createNewKeyring("MyKeyring1", cli)
    createNewKeyring("MyKeyring2", cli)


def renameKeyring(oldName, newName, cli):
    if oldName:
        cli.enterCmd("rename keyring {} to {}".format(oldName, newName))
    else:
        cli.enterCmd("rename keyring to {}".format(newName))
    assert 'Wallet {} renamed to {}'.format(oldName,
                                            newName) in cli.lastCmdOutput
    assert cli._activeWallet.name == newName
    assert len(cli.activeWallet.signers) == 0


def renameToExistingKeyring(oldName, newName, cli):
    if oldName:
        cli.enterCmd("rename keyring {} to {}".format(oldName, newName))
    else:
        cli.enterCmd("rename keyring to {}".format(newName))
    assert 'New identifier is not available, please choose a new name' in \
           cli.lastCmdOutput


def testRenameKeyring(cli):
    createNewKeyring("MyKeyring3", cli)
    createNewKeyring("MyKeyring4", cli)
    renameKeyring("MyKeyring4", "MyKeyring5", cli)
    renameToExistingKeyring("MyKeyring5", "MyKeyring3", cli)


def testKeyAndKeyRing(cli):
    cli.enterCmd("new keyring {}".format("testkr1"))
    assert 'Active wallet set to "{}"'.format("testkr1") in cli.lastCmdOutput
    assert 'New wallet {} created'.format("testkr1") in cli.lastCmdOutput

    cli.enterCmd("new keyring {}".format("testkr1"))
    assert 'New identifier is not available, please choose a new name' in \
           cli.lastCmdOutput

    cli.enterCmd("new key {}".format("testkr1"))
    assert 'New identifier is not available, please choose a new name' in \
           cli.lastCmdOutput

    cli.enterCmd("new key {}".format("testkey1"))
    assert 'Key created in wallet {}'.format("testkr1") in cli.lastCmdOutput

    cli.enterCmd("new key {}".format("testkey1"))
    assert 'Key created in wallet {}'.format("testkr1") in cli.lastCmdOutput

    cli.enterCmd("new keyring {}".format("testkey1"))
    assert 'New identifier is not available, please choose a new name' in \
           cli.lastCmdOutput

    cli.enterCmd("use identifier {}".format("testkey1"))
    assert "Current identifier set to {}".format(
        "testkey1") in cli.lastCmdOutput

    cli.enterCmd("use identifier {}".format("testkr1"))
    assert 'Active wallet set to "{}"'.format("testkr1") in cli.lastCmdOutput

    cli.enterCmd("use identifier {}".format("testkey1"))
    assert 'Current identifier set to {}'.format(
        "testkey1") in cli.lastCmdOutput

    cli.enterCmd("new keyring {}".format("testkr2"))
    assert 'Active wallet set to "{}"'.format("testkr2") in cli.lastCmdOutput
    assert 'New wallet {} created'.format("testkr2") in cli.lastCmdOutput

    cli.enterCmd("use identifier {}".format("testkey1"))
    assert 'No such identifier found' in cli.lastCmdOutput

    cli.enterCmd("use identifier {}".format("testkr1"))
    assert 'Active wallet set to "{}"'.format("testkr1") in cli.lastCmdOutput

    cli.enterCmd("use identifier {}".format("testkey1"))
    assert 'Current identifier set to {}'.format(
        "testkey1") in cli.lastCmdOutput
