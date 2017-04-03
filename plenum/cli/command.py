from plenum.common.roles import Roles
from plenum.common.transactions import PlenumTransactions


class Command:
    def __init__(self, id, title, usage, note=None, examples=None):
        self.id = id  # unique command identifier
        self.title = title  # brief explanation about the command
        self.usage = usage  # syntax with all available clauses
        self.note = note  # any additional description/note
        self.examples = examples if isinstance(examples, list) else [examples] \
            if examples else examples

    def __str__(self):
        detailIndent = "    "
        header = "\n{}\n{}\n".format(self.id, '-' * (len(self.id)))
        note = "{} note: {}\n\n".format(detailIndent, self.note) if self.note else ""
        examplesStr = '\n{}{}'.format(detailIndent, detailIndent).join(
            self.examples) if self.examples else ""
        examples = "{} example(s):\n{}    {}\n".format(
            detailIndent, detailIndent, examplesStr) \
            if len(examplesStr) else ""

        helpInfo = "{} title: {}\n\n" \
                   "{} usage: {}\n\n" \
                   "{}" \
                   "{}".format(detailIndent, self.title,
                               detailIndent, self.usage, note, examples)
        return header + helpInfo


helpCmd = Command(
    id="help",
    title="Shows this or specific help message for given command",
    usage="help [<command name>]",
    examples=["help", "help list ids"])

statusCmd = Command(
    id="status",
    title="Shows general status of the sandbox",
    usage="status")

licenseCmd = Command(
    id="license",
    title="Shows the license",
    usage="license")

exitCmd = Command(
    id="exit",
    title="Exit the command-line interface ('quit' also works)",
    usage="exit")

quitCmd = Command(
    id="quit",
    title="Exit the command-line interface ('exit' also works)",
    usage="quit")

newNodeCmd = Command(
    id="new node",
    title="Starts new node",
    usage="new node <name>",
    examples=["new node Alpha", "new node all"])

newClientCmd = Command(
    id="new client",
    title="Starts new client",
    usage="new client <name>",
    examples="new client Alice")

statusNodeCmd = Command(
    id="status node",
    title="Shows status for given node",
    usage="status node <name>",
    examples="status node Alpha")

statusClientCmd = Command(
    id="status client",
    title="Shows status for given client",
    usage="status client <name>",
    examples="status client Alice")

# TODO: Obsolete, Needs to be removed
# keyShareCmd = Command(
#     id="keyshare",
#     title="Manually starts key sharing of a node",
#     usage="keyshare node <name>",
#     examples="keyshare node Alpha")

loadPluginsCmd = Command(
    id="load plugins",
    title="load plugins from given directory",
    usage="load plugins from <dir path>",
    examples="load plugins from /home/ubuntu/plenum/plenum/test/plugin/stats_consumer")

clientSendCmd = Command(
    id="client send",
    title="Client sends a message to pool",
    usage="client <client-name> send {<json request data>}",
    examples="client Alice send {'type':'GET_NYM', 'dest':'4QxzWk3ajdnEA37NdNU5Kt'}")

clientShowCmd = Command(
    id="client show request status",
    title="Shows status of a sent request",
    usage="client <client-name> show <req-id>",
    note="This will only show status for the request sent by 'client send' command",
    examples="client Alice show 1486651494426621")

newKeyCmd = Command(
    id="new key",
    title="Adds new key to active keyring",
    usage="new key [with seed <32 character seed>] [[as] <alias>]",
    examples=[
        "new key",
        "new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa myalias",
        "new key with seed aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa as myalias"])

listIdsCmd = Command(
    id="list ids",
    title="Lists all identifiers of active keyring",
    usage="list ids [with verkeys]",
    examples=["list ids", "list ids with verkeys"])

useIdCmd = Command(
    id="use identifier",
    title="Marks given identifier active/default",
    usage="use identifier <identifier>",
    note="Note: To see all identifiers in active keyring, use 'list ids' command",
    examples="use identifier 5pJcAEAQqW7B8aGSxDArGaeXvb1G1MQwwqLMLmG2fAy9")

addGenesisTxnCmd = Command(
    id="add genesis transaction",
    title="Adds given genesis transaction",
    usage="add genesis transaction <type> for <dest-identifier> [by <identifier>] [with data {<json data>}] [role=<role>]",
    examples=[
        'add genesis transaction {nym} for 2ru5PcgeQzxF7QZYwQgDkG2K13PRqyigVw99zMYg8eML role={role}'.format(
            nym=PlenumTransactions.NYM.name, role=Roles.STEWARD.name),
        'add genesis transaction {nym} for 2ru5PcgeQzxF7QZYwQgDkG2K13PRqyigVw99zMYg8eML with data {{"alias": "Alice"}} role={role}'.format(
            nym=PlenumTransactions.NYM.name, role=Roles.STEWARD.name),
        'add genesis transaction {node} for 2ru5PcgeQzxF7QZYwQgDkG2K13PRqyigVw99zMYg8eML by FvDi9xQZd1CZitbK15BNKFbA7izCdXZjvxf91u3rQVzW '
        'with data {{"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", "client_port": "9702", "alias": "AliceNode"}}'.format(node=PlenumTransactions.NODE.name)])

createGenesisTxnFileCmd = Command(
    id="create genesis transaction file",
    title="Creates genesis transaction file with in memory genesis transaction data",
    usage="create genesis transaction file",
    examples="create genesis transaction file")

changePromptCmd = Command(
    id="prompt",
    title="Changes the prompt to given principal (a person like Alice, an organization like Faber College, or an IoT-style thing)",
    usage="prompt <principal-name>",
    examples="prompt Alice")

newKeyringCmd = Command(
    id="new keyring",
    title="Creates new keyring",
    usage="new keyring <name>",
    examples="new keyring mykeyring")

useKeyringCmd = Command(
    id="use keyring",
    title="Loads given keyring and marks it active/default",
    usage="use keyring <name|absolute-wallet-file-path>",
    examples=["use keyring mykeyring", "use keyring /home/ubuntu/.sovrin/keyrings/test/mykeyring.wallet"])

saveKeyringCmd = Command(
    id="save keyring",
    title="Saves active keyring",
    usage="save keyring [<active-keyring-name>]",
    examples=["save keyring", "save keyring mykeyring"])

renameKeyringCmd = Command(
    id="rename keyring",
    title="Renames given keyring",
    usage="rename keyring <old-name> to <new-name>",
    examples="rename keyring mykeyring to yourkeyring")

listKeyringCmd = Command(
    id="list keyrings",
    title="Lists all keyrings",
    usage="list keyrings")
