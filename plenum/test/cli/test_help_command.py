def testDefaultHelp(cli):
    """
    Testing `help` command
    """
    cli.enterCmd("help")
    defaultHelpMsgs = [
<<<<<<< HEAD
        "Plenum-CLI, a simple command-line interface for a Plenum protocol sandbox.",
        "Commands:",
        "help - Shows this or specific help message",
=======
        "Plenum-CLI, a simple command-line interface for a Plenum protocol.",
        "Commands:",
        "help - Show this or specific help message for given command",
>>>>>>> master
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
<<<<<<< HEAD
   Description = Adds new key to active keyring
   Syntax = new key [with seed <32 character seed>]
   Examples:
=======
   description = Adds new key to active keyring
   syntax = new key [with seed <32 character seed>] [[as] <alias>]
   examples:
>>>>>>> master
      new key
      new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"""
    assert newMsg in cli.lastCmdOutput


def testNewNode(cli):
    """
    Testing `help new node` command
    """
    cli.enterCmd("help new node")
    newMsg = """new node
<<<<<<< HEAD
   Description = Starts new node
   Syntax = new node <name>
   Examples:
=======
   description = Starts new node
   syntax = new node <name>
   examples:
>>>>>>> master
      new node Alpha
      new node all"""
    assert newMsg in cli.lastCmdOutput


def testNewClient(cli):
    """
    Testing `help new client` command
    """
    cli.enterCmd("help new client")
    newMsg = """new client
<<<<<<< HEAD
   Description = Starts new client
   Syntax = new client <name>
   Examples:
=======
   description = Starts new client
   syntax = new client <name>
   examples:
>>>>>>> master
      new client Alice"""
    assert newMsg in cli.lastCmdOutput
