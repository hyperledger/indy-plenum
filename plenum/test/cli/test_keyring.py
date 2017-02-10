def createNewKeyring(name, cli):
    oldKeyring = cli.activeWallet
    cli.enterCmd("new keyring {}".format(name))
    assert 'Active keyring set to "{}"'.format(name) in cli.lastCmdOutput
    assert 'New keyring {} created'.format(name) in cli.lastCmdOutput
    assert not oldKeyring or (
    oldKeyring and oldKeyring.name != cli._activeWallet.name)
    assert cli.activeWallet.name == name
    assert len(cli._activeWallet.identifiers) == 0


def testNewKeyring(cli):
    createNewKeyring("MyKeyring1", cli)
    createNewKeyring("MyKeyring2", cli)


def renameKeyring(oldName, newName, cli):
    if oldName:
        cli.enterCmd("rename keyring {} to {}".format(oldName, newName))
    else:
        cli.enterCmd("rename keyring to {}".format(newName))
    assert 'Keyring {} renamed to {}'.format(oldName,
                                            newName) in cli.lastCmdOutput
    assert cli._activeWallet.name == newName
    assert len(cli.activeWallet.identifiers) == 0


def renameToExistingKeyring(oldName, newName, cli):
    if oldName:
        cli.enterCmd("rename keyring {} to {}".format(oldName, newName))
    else:
        cli.enterCmd("rename keyring to {}".format(newName))
    assert '"{}" conflicts with an existing keyring. ' \
           'Please choose a new name'.format(newName) in \
           cli.lastCmdOutput


def testRenameKeyring(cli):
    createNewKeyring("MyKeyring3", cli)
    createNewKeyring("MyKeyring4", cli)
    renameKeyring("MyKeyring4", "MyKeyring5", cli)
    renameToExistingKeyring("MyKeyring5", "MyKeyring3", cli)


def testKeyAndKeyRing(cli):
    keyring1 = "testkr1"
    cli.enterCmd("new keyring {}".format(keyring1))
    assert 'Active keyring set to "{}"'.format(keyring1) in cli.lastCmdOutput
    assert 'New keyring {} created'.format(keyring1) in cli.lastCmdOutput
    cli.enterCmd("list keyrings")
    assert 'testkr1' in cli.lastCmdOutput

    cli.enterCmd("new key {}".format(keyring1))
    assert 'Key created in keyring {}'.format(keyring1) in cli.lastCmdOutput

    cli.enterCmd("new key {}".format(keyring1))
    assert 'Key created in keyring {}'.format(keyring1) in cli.lastCmdOutput

    key1 = "testkey1"
    cli.enterCmd("new key {}".format(key1))
    assert 'Key created in keyring {}'.format(keyring1) in cli.lastCmdOutput

    cli.enterCmd("new keyring {}".format(key1))
    assert 'Active keyring set to "{}"'.format(key1) in cli.lastCmdOutput
    assert 'New keyring {} created'.format(key1) in cli.lastCmdOutput
    cli.enterCmd("list keyrings")
    assert 'testkey1' in cli.lastCmdOutput

    cli.enterCmd("use identifier {}".format(key1))
    assert 'No such identifier found in current keyring' in cli.lastCmdOutput

    cli.enterCmd("use identifier {}".format(keyring1))
    assert 'No such identifier found in current keyring' in cli.lastCmdOutput

    keyring2 = "testkr2"
    cli.enterCmd("new keyring {}".format(keyring2))
    assert 'Active keyring set to "{}"'.format(keyring2) in cli.lastCmdOutput
    assert 'New keyring {} created'.format(keyring2) in cli.lastCmdOutput

    cli.enterCmd("use identifier {}".format(key1))
    assert 'No such identifier found in current keyring' in cli.lastCmdOutput

    cli.enterCmd("use keyring {}".format(keyring1))
    assert 'Active keyring set to "{}"'.format(keyring1) in cli.lastCmdOutput

    cli.enterCmd("use identifier {}".format(key1))
    assert 'Current identifier set to {}'.format(key1) in cli.lastCmdOutput
