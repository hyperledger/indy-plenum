
class Command:

    def __init__(self, id, title, syntax=None, examples=None):
        self.id = id                # unique command identifier
        self.title = title          # brief explanation about the command
        self.syntax = syntax        # syntax with all available clauses
        self.examples = examples if isinstance(examples, list) else [examples]

    def __str__(self):
        detailIndent = "    "
        examples = '\n{}{}'.format(detailIndent, detailIndent).join(self.examples)
        header = "\n{}\n{}\n".format(self.id, '-'*(len(self.id)))
        detail = "{} title: {}\n\n" \
                 "{} syntax: {}\n\n" \
                 "{} examples:\n" \
                 "{}    {}\n".format(detailIndent, self.title,
                                     detailIndent, self.syntax,
                                     detailIndent, detailIndent, examples)
        return header + detail

helpCmd = Command(
    id="help",
    title="Shows this or specific help message for given command",
    syntax=" [<command name>]",
    examples=["help", "help list ids"])

statusCmd = Command(
    id="status",
    title="Shows general status of the sandbox",
    syntax="status",
    examples="status")

licenseCmd = Command(
    id="license",
    title="Shows the license",
    syntax="license",
    examples="license")

exitCmd = Command(
    id="exit",
    title="Exit the command-line interface ('quit' also works)",
    syntax="exit",
    examples="exit")

quitCmd = Command(
    id="quit",
    title="Exit the command-line interface ('exit' also works)",
    syntax="quit",
    examples="quit")

listCmd = Command(
    id="list",
    title="Shows the list of all commands you can run",
    syntax="list [sorted]",
    examples=["list", "list sorted"])

newNodeCmd = Command(
    id="new node",
    title="Starts new node",
    syntax="new node <name>",
    examples=["new node Alpha", "new node all"])

newClientCmd = Command(
    id="new client",
    title="Starts new client",
    syntax="new client <name>",
    examples="new client Alice")

statusNodeCmd = Command(
    id="status node",
    title="Shows status for given node",
    syntax="status node <name>",
    examples="status node Alpha")

statusClientCmd = Command(
    id="status client",
    title="Shows status for given client",
    syntax="status client <name>",
    examples="status client Alice")

keyShareCmd = Command(
    id="keyshare",
    title="Manually starts key sharing of a node",
    syntax="keyshare node <name>",
    examples="keyshare node Alpha")

loadPluginsCmd = Command(
    id="load plugins",
    title="load plugins from given directory",
    syntax="load plugins from <dir path>",
    examples="load plugins from /home/ubuntu/plenum/plenum/test/plugin/stats_consumer")

clientSendCmd = Command(
    id="client send",
    title="Client sends a message to pool",
    syntax="client <client-name> send {<json data>}",
    examples="client Alice send {'data':'test'}")

clientShowCmd = Command(
    id="client show request status",
    title="Shows status of a sent request",
    syntax="client <client-name> show <req-id>",
    examples="client Alice show 1486651494426621")

newKeyCmd = Command(
    id="new key",
    title="Adds new key to active keyring",
    syntax="new key [with seed <32 character seed>] [[as] <alias>]",
    examples=[
        "new key",
        "new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa myalias",
        "new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa as myalias"])

listIdsCmd = Command(
    id="list ids",
    title="Lists all identifiers of active keyring",
    syntax="list ids [with verkeys]",
    examples=["list ids", "list ids with verkeys"])

useIdCmd = Command(
    id="use identifier",
    title="Marks given idetifier active/default",
    syntax="use identifier <identifier>",
    examples="use identifier 5pJcAEAQqW7B8aGSxDArGaeXvb1G1MQwwqLMLmG2fAy9")

addGenesisTxnCmd = Command(
    id="add genesis",
    title="Adds given genesis transaction",
    syntax="add genesis transaction <type> for <dest> [by <identifier>] [with data {<json data>}] [role=<role>]",
    examples=[
        'add genesis transaction NYM for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 role=STEWARD',
        'add genesis transaction NYM for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06419 with data {"alias": "Alice"} role=STEWARD',
        'add genesis transaction NODE for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 by 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 '
            'with data {"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", "client_port": "9702", "alias": "AliceNode"}'])

createGenesisTxnFileCmd = Command(
    id="create genesis txn file",
    title="Creates genesis transaction file with in memory genesis transaction data",
    syntax="creates genesis transaction file",
    examples="creates genesis transaction file")

changePromptCmd = Command(
    id="prompt",
    title="Changes the prompt to given principal (a person like Alice, an organization like Faber College, or an IoT-style thing)",
    syntax="prompt <principal-name>",
    examples="prompt Alice")

newKeyringCmd = Command(
    id="new keyring",
    title="Creates new keyring",
    syntax="new keyring <name>",
    examples="new keyring mykeyring")

useKeyringCmd = Command(
    id="use keyring",
    title="Loads given keyring and marks it active/default",
    syntax="use keyring <name|absolute-wallet-file-path>",
    examples=["use keyring mykeyring","use keyring /home/ubuntu/.sovrin/keyrings/test/mykeyring.wallet"])

saveKeyringCmd = Command(
    id="save keyring",
    title="Saves active keyring",
    syntax="save keyring [<active-keyring-name>]",
    examples=["save keyring", "save keyring mykeyring"])


renameKeyringCmd = Command(
    id="rename keyring",
    title="Renames given keyring",
    syntax="rename keyring <old-name> to <new-name>",
    examples="rename keyring mykeyring to yourkeyring")

listKeyringCmd = Command(
    id="list keyrings",
    title="Lists all keyrings",
    syntax="list keyrings",
    examples="list keyrings")
