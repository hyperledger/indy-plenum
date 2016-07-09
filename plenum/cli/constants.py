import re
import os


def relist(seq):
    return '(' + '|'.join(seq) + ')'


def getPipedRegEx(cmd):
    return cmd + "|"

psep = re.escape(os.path.sep)

# general reusable reg ex
NODE_OR_CLI = ['node',  'client']
UTIL_GRAMS_SIMPLE_CMD_REG_EX = "(\s* (?P<simple>{}) \s*) "
UTIL_GRAMS_LOAD_CMD_REG_EX = "(\s* (?P<load>load) \s+ (?P<file_name>[.a-zA-z0-9{}]+) \s*) "
UTIL_GRAMS_COMMAND_HELP_REG_EX = \
    "(\s* (?P<command>help) (\s+ (?P<helpable>[a-zA-Z0-9]+) )? (\s+ (?P<node_or_cli>{}) )?\s*) "
UTIL_GRAMS_COMMAND_LIST_REG_EX = "(\s* (?P<command>list) \s*)"

NODE_GRAMS_NODE_COMMAND_REG_EX = \
    "(\s* (?P<node_command>{}) \s+ (?P<node_or_cli>nodes?) \s+ (?P<node_name>[a-zA-Z0-9]+)\s*) "
NODE_GRAMS_LOAD_PLUGINS_REG_EX = \
    "(\s* (?P<load_plugins>load\s+plugins\s+from) \s+ (?P<plugin_dir>[a-zA-Z0-9-:{}]+) \s*)"

CLIENT_GRAMS_CLIENT_COMMAND_REG_EX = \
    "(\s* (?P<client_command>{}) \s+ (?P<node_or_cli>clients?) \s+ (?P<client_name>[a-zA-Z0-9]+) \s*) "
CLIENT_GRAMS_CLIENT_SEND_REG_EX = \
    "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>send) \s+ (?P<msg>\{\s*.*\}) \s*) "
CLIENT_GRAMS_CLIENT_SHOW_REG_EX = \
    "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>show) \s+ (?P<req_id>[0-9]+) \s*) "
CLIENT_GRAMS_ADD_KEY_REG_EX = \
    "(\s* (?P<add_key>add\s+key) \s+ (?P<verkey>[a-fA-F0-9]+) \s+ (?P<for_client>for\s+client) \s+ (?P<identifier>[a-zA-Z0-9]+) \s*) "
CLIENT_GRAMS_NEW_KEYPAIR_REG_EX = \
    "(\s* (?P<new_key>new\skey) \s* (?P<alias>[a-zA-Z0-9]+)?\s*) "
CLIENT_GRAMS_LIST_IDS_REG_EX = "(\s* (?P<list_ids>list\sids) \s*) "
CLIENT_GRAMS_BECOME_REG_EX = "(\s* (?P<become>become) \s+ (?P<id>[a-zA-Z0-9]+) \s*) "
CLIENT_GRAMS_USE_KEYPAIR_REG_EX = "(\s* (?P<use_keypair>use\skeypair) \s+ (?P<keypair>[A-Za-z0-9+=/]*) \s*) "

# commands
SIMPLE_CMDS = {'status', 'exit', 'quit', 'license'}
CLI_CMDS = {'status', 'new'}
NODE_CMDS = CLI_CMDS | {'keyshare'}


# command formatted reg exs
UTIL_GRAMS_SIMPLE_CMD_FORMATTED_REG_EX = getPipedRegEx(UTIL_GRAMS_SIMPLE_CMD_REG_EX).format(relist(SIMPLE_CMDS))
UTIL_GRAMS_LOAD_CMD_FORMATTED_REG_EX = getPipedRegEx(UTIL_GRAMS_LOAD_CMD_REG_EX).format(psep)
UTIL_GRAMS_COMMAND_HELP_FORMATTED_REG_EX = getPipedRegEx(UTIL_GRAMS_COMMAND_HELP_REG_EX).format(relist(NODE_OR_CLI))
UTIL_GRAMS_COMMAND_LIST_FORMATTED_REG_EX = UTIL_GRAMS_COMMAND_LIST_REG_EX

NODE_GRAMS_NODE_COMMAND_FORMATTED_REG_EX = getPipedRegEx(NODE_GRAMS_NODE_COMMAND_REG_EX).format(relist(NODE_CMDS))
NODE_GRAMS_LOAD_PLUGINS_FORMATTED_REG_EX = NODE_GRAMS_LOAD_PLUGINS_REG_EX.format(psep)

CLIENT_GRAMS_CLIENT_COMMAND_FORMATTED_REG_EX = getPipedRegEx(CLIENT_GRAMS_CLIENT_COMMAND_REG_EX).format(relist(CLI_CMDS))
CLIENT_GRAMS_CLIENT_SEND_FORMATTED_REG_EX = getPipedRegEx(CLIENT_GRAMS_CLIENT_SEND_REG_EX)
CLIENT_GRAMS_CLIENT_SHOW_FORMATTED_REG_EX = getPipedRegEx(CLIENT_GRAMS_CLIENT_SHOW_REG_EX)
CLIENT_GRAMS_ADD_KEY_FORMATTED_REG_EX = getPipedRegEx(CLIENT_GRAMS_ADD_KEY_REG_EX)
CLIENT_GRAMS_NEW_KEYPAIR_FORMATTED_REG_EX = getPipedRegEx(CLIENT_GRAMS_NEW_KEYPAIR_REG_EX)
CLIENT_GRAMS_LIST_IDS_FORMATTED_REG_EX = getPipedRegEx(CLIENT_GRAMS_LIST_IDS_REG_EX)
CLIENT_GRAMS_BECOME_FORMATTED_REG_EX = getPipedRegEx(CLIENT_GRAMS_BECOME_REG_EX)
CLIENT_GRAMS_USE_KEYPAIR_FORMATTED_REG_EX = CLIENT_GRAMS_USE_KEYPAIR_REG_EX

