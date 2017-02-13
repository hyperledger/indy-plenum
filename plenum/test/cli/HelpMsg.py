
class HelpMsg:

    def __init__(self, id, msg, syntax=None, *examples):
        self.id = id                # unique command identifier
        self.msg = msg              # brief explanation about the command
        self.syntax = syntax        # syntax with all available clauses
        self.examples = examples    # few examples

    def __str__(self):
        examples = '\n      '.join(self.examples)
        return "\n{}\n   description = {}\n   syntax = {}\n   " \
               "examples:\n      {}\n".format(self.id, self.msg, self.syntax, examples)


simpleHelpMsg = None

helpMsg = HelpMsg("help",
                  "Show this or specific help message for given command",
                  "help [<command name>]",
                  "help", "help list ids")

statusHelpMsg = HelpMsg("status",
                        "Show general status of the sandbox",
                        "status",
                        "status")

licenseHelpMsg = HelpMsg("license",
                         "Show the license",
                         "license",
                         "license")

exitHelpMsg = HelpMsg("exit",
                      "Exit the command-line interface ('quit' also works)",
                      "exit",
                      "exit")

quitHelpMsg = HelpMsg("quit",
                      "Exit the command-line interface ('exit' also works)",
                      "quit",
                      "quit")

listHelpMsg = HelpMsg("list",
                      "Show the list of all commands you can run",
                      "list",
                      "list")

newNodeHelpMsg = HelpMsg("new node",
                         "Starts new node",
                         "new node <name>",
                         "new node Alpha", "new node all")

newClientHelpMsg = HelpMsg("new client",
                           "Starts new client",
                           "new client <name>",
                           "new client Alice")

statusNodeHelpMsg = HelpMsg("status node",
                            "Shows status for given node",
                            "status node <name>",
                            "status node Alpha")

statusClientHelpMsg = HelpMsg("status client",
                              "Shows status for given client",
                              "status client <name>",
                              "status client Alice")

keyShareHelpMsg = HelpMsg("keyshare",
                          "Manually starts key sharing of a node",
                          "keyshare node <name>",
                          "keyshare node Alpha")

loadPlugingDirHelpMsg = HelpMsg("load plugins",
                                "Loads plugin from given directory",
                                "load plugins from <dir path>",
                                "load plugins from /home/ubuntu/plenum/plenum/test/plugin/stats_consumer")

clientSendMsgHelpMsg = HelpMsg("client send",
                               "Client sends a message to pool",
                               "client <client-name> send {<json data>}",
                               "client Alice send {'data':'test'}")

clientShowMsgHelpMsg = HelpMsg("client show request status",
                               "Show status of a sent request",
                               "client <client-name> show <req-id>",
                               "client Alice show 1486651494426621")

newKeyHelpMsg = HelpMsg("new key",
                        "Adds new key to active keyring",
                        "new key [with seed <32 character seed>] [[as] <alias>]",
                        "new key",
                        "new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa myalias",
                        "new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa as myalias")

listIdsHelpMsg = HelpMsg("list ids",
                         "Lists all identifiers of active keyring",
                         "list ids",
                         "list ids")

useIdHelpMsg = HelpMsg("use ids",
                       "Marks given idetifier active/default",
                       "use identifier <identifier>",
                       "use identifier 5pJcAEAQqW7B8aGSxDArGaeXvb1G1MQwwqLMLmG2fAy9")

addGenesisTxnHelpMsg = HelpMsg("add genesis",
                               "Adds given genesis transaction",
                               "add genesis transaction <type> for <dest> [by <identifier>] [with data {<json data>}] [role=<role>]",
                               'add genesis transaction NYM for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 role=STEWARD',
                               'add genesis transaction NYM for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06419 with data {"alias": "Alice"} role=STEWARD',
                               'add genesis transaction NODE for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 by 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 with data '
                               '{"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", "client_port": "9702", "alias": "AliceNode"}'
                               )

createGenesisTxnFileHelpMsg = HelpMsg("create genesis txn file",
                                      "Creates genesis transaction file with in memory genesis transaction data",
                                      "creates genesis transaction file",
                                      "creates genesis transaction file")

changePromptHelpMsg = HelpMsg("prompt",
                              "Changes the prompt to given principal (a person like Alice, an organization like Faber College, or an IoT-style thing)",
                              "prompt <principal-name>",
                              "prompt Alice")

newKeyringHelpMsg = HelpMsg("new keyring",
                            "Creates new keyring",
                            "new keyring <name>",
                            "new keyring mykeyring")

useKeyringHelpMsg = HelpMsg("use keyring",
                            "Loads given keyring and marks it active/default",
                            "use keyring <name|absolute-wallet-file-path>",
                            "use keyring mykeyring","use keyring /home/ubuntu/.sovrin/keyrings/test/mykeyring.wallet")

saveKeyringHelpMsg = HelpMsg("save keyring",
                             "Saves active keyring",
                             "save keyring [<active-keyring-name>]",
                             "save keyring", "save keyring mykeyring")


renameKeyringHelpMsg = HelpMsg("rename keyring",
                               "Renames given keyring",
                               "rename keyring <old-name> to <new-name>",
                               "rename keyring mykeyring to yourkeyring")

listKeyringHelpMsg = HelpMsg("list keyrings",
                             "Lists all keyrings",
                             "list keyrings",
                             "list keyrings")
