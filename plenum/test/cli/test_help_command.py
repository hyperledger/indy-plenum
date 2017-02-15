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
     description: Adds new key to active keyring

     syntax: new key [with seed <32 character seed>] [[as] <alias>]

     examples:
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
     description: Starts new node

     syntax: new node <name>

     examples:
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
     description: Starts new client

     syntax: new client <name>

     examples:
        new client Alice"""
    assert newMsg in cli.lastCmdOutput
