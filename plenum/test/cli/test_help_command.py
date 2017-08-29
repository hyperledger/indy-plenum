def testDefaultHelp(cli):
    """
    Testing `help` command
    """
    cli.enterCmd("help")
    defaultHelpMsgs = [
        "Plenum-CLI, a simple command-line interface for a Plenum protocol.",
        "Commands:",
        "help - Shows this or specific help message for given command",
        "license - Shows the license",
        "exit - Exit the command-line interface ('quit' also works)"
    ]

    for dhm in defaultHelpMsgs:
        assert dhm in cli.lastCmdOutput


def testNewKey(cli):
    """
    Testing `help new` command
    """
    cli.enterCmd("help new key")
    newMsg = """new key
-------
     title: Adds new key to active wallet

     usage: new key [with seed <32 character seed>] [[as] <alias>]

     example(s):
        new key
        new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
        new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa myalias
        new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa as myalias"""
    assert newMsg in cli.lastCmdOutput


def testNewNode(cli):
    """
    Testing `help new node` command
    """
    cli.enterCmd("help new node")
    newMsg = """new node
--------
     title: Starts new node

     usage: new node <name>

     example(s):
        new node Alpha
        new node all"""
    assert newMsg in cli.lastCmdOutput


def testNewClient(cli):
    """
    Testing `help new client` command
    """
    cli.enterCmd("help new client")
    newMsg = """new client
----------
     title: Starts new client

     usage: new client <name>

     example(s):
        new client Alice"""
    assert newMsg in cli.lastCmdOutput
