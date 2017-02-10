def testDefaultHelp(cli):
    """
    Testing `help` command
    """
    cli.enterCmd("help")
    defaultHelpMsgs = [
        "Plenum-CLI, a simple command-line interface for a Plenum protocol sandbox.",
        "Commands:",
        "help - Shows this or specific help message",
        "license - Show the license",
        "exit - Exit the command-line interface ('quit' also works)",
        "quit - Exit the command-line interface ('exit' also works)"
        ]

    for dhm in defaultHelpMsgs:
        assert dhm in cli.lastCmdOutput


def testNewKey(cli):
    """
    Testing `help new` command
    """
    cli.enterCmd("help new key")
    newMsg = """new key
   Description = Adds new key to active keyring
   Syntax = new key [with seed <32 character seed>]
   Examples:
      new key
      new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"""
    assert newMsg in cli.lastCmdOutput


def testNewNode(cli):
    """
    Testing `help new node` command
    """
    cli.enterCmd("help new node")
    newMsg = """new node
   Description = Starts new node
   Syntax = new node <name>
   Examples:
      new node Alpha
      new node all"""
    assert newMsg in cli.lastCmdOutput


def testNewClient(cli):
    """
    Testing `help new client` command
    """
    cli.enterCmd("help new client")
    newMsg = """new client
   Description = Starts new client
   Syntax = new client <name>
   Examples:
      new client Alice"""
    assert newMsg in cli.lastCmdOutput
