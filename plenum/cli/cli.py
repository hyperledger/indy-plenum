from __future__ import unicode_literals

# noinspection PyUnresolvedReferences
import random
from hashlib import sha256
import shutil
from typing import Dict

from jsonpickle import json

from prompt_toolkit.utils import is_windows, is_conemu_ansi
import pyorient
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.stores.file_hash_store import FileHashStore
from ledger.ledger import Ledger

from plenum.cli.helper import getUtilGrams, getNodeGrams, getClientGrams, \
    getAllGrams
from plenum.cli.constants import SIMPLE_CMDS, CLI_CMDS, NODE_OR_CLI, NODE_CMDS
from plenum.common.signer_simple import SimpleSigner
from plenum.client.wallet import Wallet
from plenum.common.plugin_helper import loadPlugins
from plenum.common.raet import getLocalEstateData
from plenum.common.raet import isLocalKeepSetup
from plenum.common.txn import TXN_TYPE, TARGET_NYM, TXN_ID, DATA, IDENTIFIER, \
    NEW_NODE, ALIAS, NODE_IP, NODE_PORT, CLIENT_PORT, CLIENT_IP, VERKEY, BY

if is_windows():
    from prompt_toolkit.terminal.win32_output import Win32Output
    from prompt_toolkit.terminal.conemu_output import ConEmuOutput
else:
    from prompt_toolkit.terminal.vt100_output import Vt100_Output

import configparser
import os
from configparser import ConfigParser
from collections import OrderedDict
import time
import ast

from functools import reduce, partial
import logging
import sys
from collections import defaultdict

from prompt_toolkit.history import FileHistory
from ioflo.aid.consoling import Console
from prompt_toolkit.contrib.completers import WordCompleter
from prompt_toolkit.contrib.regular_languages.compiler import compile
from prompt_toolkit.contrib.regular_languages.completion import GrammarCompleter
from prompt_toolkit.contrib.regular_languages.lexer import GrammarLexer
from prompt_toolkit.interface import CommandLineInterface
from prompt_toolkit.shortcuts import create_prompt_application, \
    create_asyncio_eventloop
from prompt_toolkit.layout.lexers import SimpleLexer
from prompt_toolkit.styles import PygmentsStyle
from prompt_toolkit.terminal.vt100_output import Vt100_Output
from pygments.token import Token
from plenum.client.client import Client
from plenum.common.util import getMaxFailures, checkPortAvailable, \
    firstValue, randomString, cleanSeed, bootstrapClientKeys
from plenum.common.log import CliHandler, getlogger, setupLogging, \
    getRAETLogLevelFromConfig, getRAETLogFilePath, TRACE_LOG_LEVEL
from plenum.server.node import Node
from plenum.common.types import CLIENT_STACK_SUFFIX, NodeDetail, HA
from plenum.server.plugin_loader import PluginLoader
from plenum.server.replica import Replica
from plenum.common.util import getConfig, hexToFriendly


class CustomOutput(Vt100_Output):
    """
    Subclassing Vt100 just to override the `ask_for_cpr` method which prints
    an escape character on the console. Not printing the escape character
    """

    def ask_for_cpr(self):
        """
        Asks for a cursor position report (CPR).
        """
        self.flush()


class Cli:
    isElectionStarted = False
    primariesSelected = 0
    electedPrimaries = set()
    name = 'plenum'
    properName = 'Plenum'
    fullName = 'Plenum protocol'

    NodeClass = Node
    ClientClass = Client
    defaultWalletName = 'Default'

    _genesisTransactions = []

    # noinspection PyPep8
    def __init__(self, looper, basedirpath, nodeReg=None, cliNodeReg=None,
                 output=None, debug=False, logFileName=None, config=None,
                 useNodeReg=False):
        self.curClientPort = None
        logging.root.addHandler(CliHandler(self.out))
        self.looper = looper
        self.basedirpath = os.path.expanduser(basedirpath)
        self.nodeRegLoadedFromFile = False
        self.config = config or getConfig(self.basedirpath)
        if not (useNodeReg and
                    nodeReg and len(nodeReg) and cliNodeReg and len(cliNodeReg)):
            self.nodeRegLoadedFromFile = True
            nodeReg = {}
            cliNodeReg = {}
            dataDir = self.basedirpath
            ledger = Ledger(CompactMerkleTree(hashStore=FileHashStore(
                dataDir=dataDir)),
                dataDir=dataDir,
                fileName=self.config.poolTransactionsFile)
            for _, txn in ledger.getAllTxn().items():
                if txn[TXN_TYPE] == NEW_NODE:
                    nodeName = txn[DATA][ALIAS]
                    nHa = (txn[DATA][NODE_IP], txn[DATA][NODE_PORT]) \
                        if (NODE_IP in txn[DATA] and NODE_PORT in txn[DATA]) \
                        else None
                    cHa = (txn[DATA][CLIENT_IP], txn[DATA][CLIENT_PORT]) \
                        if (CLIENT_IP in txn[DATA] and CLIENT_PORT in txn[DATA]) \
                        else None
                    if nHa:
                        nodeReg[nodeName] = HA(*nHa)
                    if cHa:
                        cliNodeReg[nodeName + CLIENT_STACK_SUFFIX] = HA(*cHa)
        self.nodeReg = nodeReg
        self.cliNodeReg = cliNodeReg
        self.nodeRegistry = {}
        for nStkNm, nha in self.nodeReg.items():
            cStkNm = nStkNm + CLIENT_STACK_SUFFIX
            self.nodeRegistry[nStkNm] = NodeDetail(HA(*nha), cStkNm,
                                                   HA(*self.cliNodeReg[cStkNm]))
        # Used to store created clients
        self.clients = {}  # clientName -> Client
        # To store the created requests
        self.requests = {}
        # To store the nodes created
        self.nodes = {}
        self.externalClientKeys = {}  # type: Dict[str,str]

        self.cliCmds = CLI_CMDS
        self.nodeCmds = NODE_CMDS
        self.helpablesCommands = self.cliCmds | self.nodeCmds
        self.simpleCmds = SIMPLE_CMDS
        self.commands = {'list', 'help'} | self.simpleCmds
        self.cliActions = {'send', 'show'}
        self.commands.update(self.cliCmds)
        self.commands.update(self.nodeCmds)
        self.node_or_cli = NODE_OR_CLI
        self.nodeNames = list(self.nodeReg.keys()) + ["all"]
        self.debug = debug
        self.plugins = {}
        self.pluginPaths = []
        self.defaultClient = None
        self.activeIdentifier = None
        # Wallet and Client are the same from user perspective for now
        self._activeClient = None
        self._wallets = {}  # type: Dict[str, Wallet]
        self._activeWallet = None  # type: Wallet
        self.keyPairs = {}
        '''
        examples:
        status

        new node Alpha
        new node all
        new client Joe
        client Joe send <msg>
        client Joe show 1
        '''

        self.utilGrams = getUtilGrams()

        self.nodeGrams = getNodeGrams()

        self.clientGrams = getClientGrams()

        self._allGrams = []

        self._lexers = {}

        self.clientWC = WordCompleter([])

        self._completers = {}

        self.initializeInputParser()

        self.style = PygmentsStyle.from_defaults({
            Token.Operator: '#33aa33 bold',
            Token.Number: '#aa3333 bold',
            Token.Name: '#ffff00 bold',
            Token.Heading: 'bold',
            Token.TrailingInput: 'bg:#662222 #ffffff',
            Token.BoldGreen: '#33aa33 bold',
            Token.BoldOrange: '#ff4f2f bold',
            Token.BoldBlue: '#095cab bold'})

        self.functionMappings = self.createFunctionMappings()

        self.voidMsg = "<none>"

        # Create an asyncio `EventLoop` object. This is a wrapper around the
        # asyncio loop that can be passed into prompt_toolkit.
        eventloop = create_asyncio_eventloop(looper.loop)

        self.pers_hist = FileHistory('.{}-cli-history'.format(self.name))

        # Create interface.
        app = create_prompt_application('{}> '.format(self.name),
                                        lexer=self.grammarLexer,
                                        completer=self.grammarCompleter,
                                        style=self.style,
                                        history=self.pers_hist)
        self.currPromptText = self.name

        if output:
            out = output
        else:
            if is_windows():
                if is_conemu_ansi():
                    out = ConEmuOutput(sys.__stdout__)
                else:
                    out = Win32Output(sys.__stdout__)
            else:
                out = CustomOutput.from_pty(sys.__stdout__, true_color=True)

        self.cli = CommandLineInterface(
            application=app,
            eventloop=eventloop,
            output=out)

        RAETVerbosity = getRAETLogLevelFromConfig("RAETLogLevelCli",
                                                  Console.Wordage.mute,
                                                  self.config)
        RAETLogFile = getRAETLogFilePath("RAETLogFilePathCli", self.config)
        # Patch stdout in something that will always print *above* the prompt
        # when something is written to stdout.
        sys.stdout = self.cli.stdout_proxy()
        setupLogging(TRACE_LOG_LEVEL,
                     RAETVerbosity,
                     filename=logFileName,
                     raet_log_file=RAETLogFile)

        self.logger = getlogger("cli")
        self.print("\n{}-CLI (c) 2016 Evernym, Inc.".format(self.properName))
        if nodeReg:
            self.print("Node registry loaded.")
            # self.print("None of these are created or running yet.")

            self.showNodeRegistry()
        self.print("Type 'help' for more information.")
        self._actions = []

        tp = loadPlugins(self.basedirpath)
        self.logger.debug("total plugins loaded in cli: {}".format(tp))

        # TODO commented out by JAL, DON'T COMMIT
        # uncommented by JN.
        # self.ensureDefaultClientCreated()
        # self.print("Current wallet set to {}".format(self.defaultClient.name))
        # alias, signer = next(iter(self.defaultClient.wallet.signers.items()))
        # self.print("Current identifier set to {alias} ({cryptonym})".format(
        #     alias=alias, cryptonym=signer.verstr))

    @property
    def genesisTransactions(self):
        return self._genesisTransactions

    def reset(self):
        self._genesisTransactions = []

    @property
    def actions(self):
        if not self._actions:
            self._actions = [self._simpleAction, self._helpAction,
                             self._listAction,
                             self._newNodeAction, self._newClientAction,
                             self._statusNodeAction, self._statusClientAction,
                             self._keyShareAction, self._loadPluginDirAction,
                             self._clientCommand, self._addKeyAction,
                             self._newKeyAction, self._listIdsAction,
                             self._useIdentifierAction, self._addGenesisAction,
                             self._createGenTxnFileAction, self._changePrompt,
                             self._newKeyring, self._renameKeyring,
                             self._useKeyringAction]
        return self._actions

    @property
    def allGrams(self):
        if not self._allGrams:
            self._allGrams = [self.utilGrams, self.nodeGrams, self.clientGrams]
        return self._allGrams

    @property
    def completers(self):
        if not self._completers:
            self._completers = {
                'node_command': WordCompleter(self.nodeCmds),
                'client_command': WordCompleter(self.cliCmds),
                'client': WordCompleter(['client']),
                'command': WordCompleter(self.commands),
                'node_or_cli': WordCompleter(self.node_or_cli),
                'node_name': WordCompleter(self.nodeNames),
                'more_nodes': WordCompleter(self.nodeNames),
                'helpable': WordCompleter(self.helpablesCommands),
                'load_plugins': WordCompleter(['load plugins from']),
                'client_name': self.clientWC,
                'second_client_name': self.clientWC,
                'cli_action': WordCompleter(self.cliActions),
                'simple': WordCompleter(self.simpleCmds),
                'add_key': WordCompleter(['add key']),
                'for_client': WordCompleter(['for client']),
                'new_key': WordCompleter(['new', 'key']),
                'new_keyring': WordCompleter(['new', 'keyring']),
                'rename_keyring': WordCompleter(['rename', 'keyring']),
                'list_ids': WordCompleter(['list', 'ids']),
                'become': WordCompleter(['become']),
                'use_id': WordCompleter(['use', 'identifier']),
                'use_kr': WordCompleter(['use', 'keyring']),
                'add_gen_txn': WordCompleter(['add', 'genesis', 'transaction']),
                'prompt': WordCompleter(['prompt']),
                'create_gen_txn_file': WordCompleter(
                    ['create', 'genesis', 'transaction', 'file'])
            }
        return self._completers

    @property
    def lexers(self):
        if not self._lexers:
            lexerNames = {
                'node_command',
                'command',
                'helpable',
                'load_plugins',
                'load',
                'node_or_cli',
                'arg1',
                'node_name',
                'more_nodes',
                'simple',
                'client_command',
                'add_key',
                'verkey',
                'for_client',
                'identifier',
                'new_key',
                'list_ids',
                'become',
                'use_id',
                'prompt',
                'new_keyring',
                'rename_keyring',
                'add_genesis',
                'create_gen_txn_file'
            }
            lexers = {n: SimpleLexer(Token.Keyword) for n in lexerNames}
            self._lexers = {**lexers}
        return self._lexers

    def _renameKeyring(self, matchedVars):
        if matchedVars.get('rename_keyring'):
            fromName = matchedVars.get('from')
            toName = matchedVars.get('to')
            conflictFound = self._checkIfIdentifierConflicts(toName)
            if not conflictFound:
                fromWallet = self.wallets.get(fromName) if fromName \
                    else self.activeWallet
                if not fromWallet:
                    self.print('Keyring {} not found'.format(fromName))
                    return True
                fromWallet.name = toName
                del self.wallets[fromName]
                self.wallets[toName] = fromWallet
                self.print('Keyring {} renamed to {}'.format(fromName, toName))
            return True

    def _newKeyring(self, matchedVars):
        if matchedVars.get('new_keyring'):
            name = matchedVars.get('name')
            conflictFound = self._checkIfIdentifierConflicts(
                name, checkInAliases=False, checkInSigners=False)
            if not conflictFound:
                self._newWallet(name)
            return True

    def _changePrompt(self, matchedVars):
        if matchedVars.get('prompt'):
            promptText = matchedVars.get('name')
            self._setPrompt(promptText)
            return True

    def _createGenTxnFileAction(self, matchedVars):
        if matchedVars.get('create_gen_txn_file'):
            ledger = Ledger(CompactMerkleTree(),
                            dataDir=self.basedirpath,
                            fileName=self.config.poolTransactionsFile)
            ledger.reset()
            for item in self.genesisTransactions:
                ledger.add(item)
            self.print('Genesis transaction file created at {} '
                       .format(ledger._transactionLog.dbPath))
            return True

    def _addGenesisAction(self, matchedVars):
        if matchedVars.get('add_gen_txn'):
            if matchedVars.get(TARGET_NYM):
                return self._addOldGenesisCommand(matchedVars)
            else:
                return self._addNewGenesisCommand(matchedVars)

    def _addNewGenesisCommand(self, matchedVars):
        typ = matchedVars.get(TXN_TYPE)

        nodeName, nodeData, identifier = None, None, None
        jsonNodeData = json.loads(matchedVars.get(DATA))
        for key, value in jsonNodeData.items():
            if key == BY:
                identifier = value
            else:
                nodeName, nodeData = key, value

        withData = {ALIAS: nodeName}

        if typ == NEW_NODE:
            nodeIp, nodePort = nodeData.get('node_address').split(':')
            clientIp, clientPort = nodeData.get('client_address').split(':')
            withData[NODE_IP] = nodeIp
            withData[NODE_PORT] = int(nodePort)
            withData[CLIENT_IP] = clientIp
            withData[CLIENT_PORT] = int(clientPort)

        newMatchedVars = {TXN_TYPE: typ, DATA: json.dumps(withData),
                          TARGET_NYM: nodeData.get(VERKEY),
                          IDENTIFIER: identifier}
        return self._addOldGenesisCommand(newMatchedVars)

    def _addOldGenesisCommand(self, matchedVars):
        destId = hexToFriendly(matchedVars.get(TARGET_NYM))
        typ = matchedVars.get(TXN_TYPE)
        txn = {
            TXN_TYPE: typ,
            TARGET_NYM: destId,
            TXN_ID: sha256(randomString(6).encode()).hexdigest(),
        }
        if matchedVars.get(IDENTIFIER):
            txn[IDENTIFIER] = hexToFriendly(matchedVars.get(IDENTIFIER))

        if matchedVars.get(DATA):
            txn[DATA] = json.loads(matchedVars.get(DATA))

        self.genesisTransactions.append(txn)
        self.print('Genesis transaction added')
        return True

    def _buildClientIfNotExists(self, config=None):
        if not self._activeClient:
            if not self.activeWallet:
                print("Keyring is not initialized")
                return
            # Need a unique name so nodes can differentiate
            name = self.name + randomString(6)
            self.newClient(clientName=name, config=config)

    @property
    def wallets(self):
        return self._wallets

    @property
    def activeWallet(self) -> Wallet:
        if not self._activeWallet:
            if self.wallets:
                return firstValue(self.wallets)
            else:
                return self._newWallet()
        return self._activeWallet

    @activeWallet.setter
    def activeWallet(self, wallet):
        self._activeWallet = wallet
        self.print('Active keyring set to "{}"'.format(wallet.name))

    @property
    def activeClient(self):
        self._buildClientIfNotExists()
        return self._activeClient

    @activeClient.setter
    def activeClient(self, client):
        self._activeClient = client
        self.print("Active client set to " + client.name)

    @staticmethod
    def relist(seq):
        return '(' + '|'.join(seq) + ')'

    def initializeInputParser(self):
        self.initializeGrammar()
        self.initializeGrammarLexer()
        self.initializeGrammarCompleter()

    def initializeGrammar(self):
        # TODO Do we really need both self.allGrams and self.grams
        self.grams = getAllGrams(*self.allGrams)
        self.grammar = compile("".join(self.grams))

    def initializeGrammarLexer(self):
        self.grammarLexer = GrammarLexer(self.grammar, lexers=self.lexers)

    def initializeGrammarCompleter(self):
        self.grammarCompleter = GrammarCompleter(self.grammar, self.completers)

    def createFunctionMappings(self):

        def newHelper():
            self.print("""Is used to create a new node or a client.
                     Usage: new <node/client> <nodeName/clientName>""")

        def statusHelper():
            self.print("status command helper")

        def nodeHelper():
            self.print("It is used to create a new node")

        def clientHelper():
            self.print("It is used to create a new client")

        def statusNodeHelper():
            self.print("It is used to check status of a created node")

        def statusClientHelper():
            self.print("It is used to check status of a created client")

        def listHelper():
            self.print("List all the commands, you can use in this CLI.")

        def exitHelper():
            self.print("Exits the CLI")

        def licenseHelper():
            self.print("""
                        Copyright 2016 Evernym, Inc.
        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.
            """)

        def sendHelper():
            self.print("""Used to send a message from a client to nodes"
                     Usage: client <clientName> send <{Message}>""")

        def showHelper():
            self.print("""Used to show status of request sent by the client"
                     Usage: client <clientName> show <reqID>""")

        def defaultHelper():
            self.printHelp()

        def pluginHelper():
            self.print("""Used to load a plugin from a given directory
                        Usage: load plugins from <dir>""")

        mappings = {
            'new': newHelper,
            'status': statusHelper,
            'list': listHelper,
            'newnode': nodeHelper,
            'newclient': clientHelper,
            'statusnode': statusNodeHelper,
            'statusclient': statusClientHelper,
            'license': licenseHelper,
            'send': sendHelper,
            'show': showHelper,
            'exit': exitHelper,
            'plugins': pluginHelper
        }

        return defaultdict(lambda: defaultHelper, **mappings)

    def print(self, msg, token=None, newline=True):
        if newline:
            msg += "\n"
        part = partial(self.cli.print_tokens, [(token, msg)])
        if self.debug:
            part()
        else:
            self.cli.run_in_terminal(part)

    def printVoid(self):
        self.print(self.voidMsg)

    def out(self, record, extra_cli_value=None):
        """
        Callback so that this cli can manage colors

        :param record: a log record served up from a custom handler
        :param extra_cli_value: the "cli" value in the extra dictionary
        :return:
        """
        if extra_cli_value in ("IMPORTANT", "ANNOUNCE"):
            self.print(record.msg, Token.BoldGreen)  # green
        elif extra_cli_value in ("WARNING",):
            self.print(record.msg, Token.BoldOrange)  # orange
        elif extra_cli_value in ("STATUS",):
            self.print(record.msg, Token.BoldBlue)  # blue
        elif extra_cli_value in ("PLAIN", "LOW_STATUS"):
            self.print(record.msg, Token)  # white
        else:
            self.print(record.msg, Token)

    def printHelp(self):
        self.print("""{}-CLI, a simple command-line interface for a {} sandbox.
        Commands:
            help - Shows this help message
            help <command> - Shows the help message of <command>
            new - creates one or more new nodes or clients
            keyshare - manually starts key sharing of a node
            status - Shows general status of the sandbox
            status <node_name>|<client_name> - Shows specific status
            list - Shows the list of commands you can run
            license - Show the license
            exit - exit the command-line interface ('quit' also works)""".
                format(self.properName, self.fullName))

    def printCmdHelper(self, command=None):
        self.functionMappings[command]()

    @staticmethod
    def joinTokens(tokens, separator=None, begin=None, end=None):
        if separator is None:
            separator = (Token, ', ')
        elif isinstance(separator, str):
            separator = (Token, separator)
        r = reduce(lambda x, y: x + [separator, y] if x else [y], tokens, [])
        if begin is not None:
            b = (Token, begin) if isinstance(begin, str) else begin
            r = [b] + r
        if end:
            if isinstance(end, str):
                r.append((Token, end))
        return r

    def printTokens(self, tokens, separator=None, begin=None, end=None):
        x = self.joinTokens(tokens, separator, begin, end)
        self.cli.print_tokens(x, style=self.style)

    def printNames(self, names, newline=False):
        tokens = [(Token.Name, n) for n in names]
        self.printTokens(tokens)
        if newline:
            self.printTokens([(Token, "\n")])

    def showValidNodes(self):
        self.printTokens([(Token, "Valid node names are: ")])
        self.printNames(self.nodeReg.keys(), newline=True)

    def showNodeRegistry(self):
        t = []
        for name in self.nodeReg:
            ip, port = self.nodeReg[name]
            t.append((Token.Name, "    " + name))
            t.append((Token, ": {}:{}\n".format(ip, port)))
        self.cli.print_tokens(t, style=self.style)

    def loadFromFile(self, file: str) -> None:
        cfg = ConfigParser()
        cfg.read(file)
        self.nodeReg = Cli.loadNodeReg(cfg)
        self.cliNodeReg = Cli.loadCliNodeReg(cfg)

    @classmethod
    def loadNodeReg(cls, cfg: ConfigParser) -> OrderedDict:
        return cls._loadRegistry(cfg, 'node_reg')

    @classmethod
    def loadCliNodeReg(cls, cfg: ConfigParser) -> OrderedDict:
        try:
            return cls._loadRegistry(cfg, 'client_node_reg')
        except configparser.NoSectionError:
            return OrderedDict()

    @classmethod
    def _loadRegistry(cls, cfg: ConfigParser, reg: str):
        registry = OrderedDict()
        for n in cfg.items(reg):
            host, port = n[1].split()
            registry.update({n[0]: (host, int(port))})
        return registry

    def getStatus(self):
        self.print('Nodes: ', newline=False)
        if not self.nodes:
            self.print("No nodes are running. Try typing 'new node <name>'.")
        else:
            self.printNames(self.nodes, newline=True)
        if not self.clients:
            clients = "No clients are running. Try typing 'new client <name>'."
        else:
            clients = ",".join(self.clients.keys())
        self.print("Clients: " + clients)
        f = getMaxFailures(len(self.nodes))
        self.print("f-value (number of possible faulty nodes): {}".format(f))
        if f != 0 and len(self.nodes) >= 2 * f + 1:
            node = list(self.nodes.values())[0]
            mPrimary = node.replicas[node.instances.masterId].primaryName
            bPrimary = node.replicas[node.instances.backupIds[0]].primaryName
            self.print("Instances: {}".format(f + 1))
            self.print("   Master (primary is on {})".
                       format(Replica.getNodeName(mPrimary)))
            self.print("   Backup (primary is on {})".
                       format(Replica.getNodeName(bPrimary)))
        else:
            self.print("Instances: "
                       "Not enough nodes to create protocol instances")

    def keyshare(self, nodeName):
        node = self.nodes.get(nodeName, None)
        if node is not None:
            node = self.nodes[nodeName]
            node.startKeySharing()
        elif nodeName not in self.nodeReg:
            tokens = [(Token.Error, "Invalid node name '{}'.".format(nodeName))]
            self.printTokens(tokens)
            self.showValidNodes()
            return
        else:
            tokens = [(Token.Error, "Node '{}' not started.".format(nodeName))]
            self.printTokens(tokens)
            self.showStartedNodes()
            return

    def showStartedNodes(self):
        self.printTokens([(Token, "Started nodes are: ")])
        startedNodes = self.nodes.keys()
        if startedNodes:
            self.printNames(self.nodes.keys(), newline=True)
        else:
            self.print("None", newline=True)

    def newNode(self, nodeName: str):
        if nodeName in self.nodes:
            self.print("Node {} already exists.".format(nodeName))
            return

        if nodeName == "all":
            names = set(self.nodeReg.keys()) - set(self.nodes.keys())
        elif nodeName not in self.nodeReg:
            tokens = [
                (Token.Error, "Invalid node name '{}'. ".format(nodeName))]
            self.printTokens(tokens)
            self.showValidNodes()
            return
        else:
            names = [nodeName]

        nodes = []
        for name in names:
            node = self.NodeClass(name,
                                  nodeRegistry=None if self.nodeRegLoadedFromFile
                                  else self.nodeRegistry,
                                  basedirpath=self.basedirpath,
                                  pluginPaths=self.pluginPaths)
            # sleep(60)
            self.nodes[name] = node
            self.looper.add(node)
            if not self.nodeRegLoadedFromFile:
                node.startKeySharing()
            for client in self.clients.values():
                # TODO: need a way to specify an identifier for a client with
                # multiple signers
                self.bootstrapClientKey(client, node)
            for identifier, verkey in self.externalClientKeys.items():
                node.clientAuthNr.addClient(identifier, verkey)
            nodes.append(node)
        return nodes

    def ensureValidClientId(self, clientName):
        """
        Ensures client id is not already used or is not starting with node
        names.

        :param clientName:
        :return:
        """
        if clientName in self.clients:
            raise ValueError("Client {} already exists.".format(clientName))

        if any([clientName.startswith(nm) for nm in self.nodeNames]):
            raise ValueError("Client name cannot start with node names, "
                             "which are {}."
                             .format(', '.join(self.nodeReg.keys())))

    def statusClient(self, clientName):
        if clientName == "all":
            for nm in self.clients:
                self.statusClient(nm)
            return
        if clientName not in self.clients:
            self.print("client not found", Token.Error)
        else:
            self.print("    Name: " + clientName)
            client = self.clients[clientName]  # type: Client

            self.printTokens([(Token.Heading, 'Status for client:'),
                              (Token.Name, client.name)],
                             separator=' ', end='\n')
            self.print("    age (seconds): {:.0f}".format(
                time.perf_counter() - client.created))
            self.print("    status: {}".format(client.status.name))
            self.print("    connected to: ", newline=False)
            if client.nodestack.conns:
                self.printNames(client.nodestack.conns, newline=True)
            else:
                self.printVoid()
            if self.activeWallet and self.activeWallet.defaultId:
                wallet = self.activeWallet
                self.print("    Identifier: {}".format(wallet.defaultId))
                self.print(
                    "    Verification key: {}".
                        format(wallet.getVerkey(wallet.defaultId)))

    def statusNode(self, nodeName):
        if nodeName == "all":
            for nm in self.nodes:
                self.statusNode(nm)
            return
        if nodeName not in self.nodes:
            self.print("Node {} not found".format(nodeName), Token.Error)
        else:
            self.print("\n    Name: " + nodeName)
            node = self.nodes[nodeName]  # type: Node
            ip, port = self.nodeReg.get(nodeName)
            nha = "0.0.0.0:{}".format(port)
            self.print("    Node listener: " + nha)
            ip, port = self.cliNodeReg.get(nodeName + CLIENT_STACK_SUFFIX)
            cha = "0.0.0.0:{}".format(port)
            self.print("    Client listener: " + cha)
            self.print("    Status: {}".format(node.status.name))
            self.print('    Connections: ', newline=False)
            connecteds = node.nodestack.connecteds
            if connecteds:
                self.printNames(connecteds, newline=True)
            else:
                self.printVoid()
            notConnecteds = list({r for r in self.nodes.keys()
                                  if r not in connecteds
                                  and r != nodeName})
            if notConnecteds:
                self.print('    Not connected: ', newline=False)
                self.printNames(notConnecteds, newline=True)
            self.print("    Replicas: {}".format(len(node.replicas)),
                       newline=False)
            if node.hasPrimary:
                if node.primaryReplicaNo == 0:
                    self.print("  (primary of Master)")
                else:
                    self.print("  (primary of Backup)")
            else:
                self.print("   (no primary replicas)")
            self.print("    Up time (seconds): {:.0f}".
                       format(time.perf_counter() - node.created))
            self.print("    Clients: ", newline=False)
            clients = node.clientstack.connecteds
            if clients:
                self.printNames(clients, newline=True)
            else:
                self.printVoid()

    def newClient(self, clientName,
                  config=None):
        try:
            self.ensureValidClientId(clientName)
            if not isLocalKeepSetup(clientName, self.basedirpath):
                client_addr = self.nextAvailableClientAddr()
            else:
                client_addr = tuple(getLocalEstateData(clientName,
                                                       self.basedirpath)['ha'])
            nodeReg = None if self.nodeRegLoadedFromFile else self.cliNodeReg
            client = self.ClientClass(clientName,
                                      ha=client_addr,
                                      nodeReg=nodeReg,
                                      basedirpath=self.basedirpath,
                                      config=config)
            self.activeClient = client
            self.looper.add(client)
            self.clients[clientName] = client
            self.clientWC.words = list(self.clients.keys())
            return client
        except ValueError as ve:
            self.print(ve.args[0], Token.Error)

    @staticmethod
    def bootstrapKey(wallet, node, identifier=None):
        identifier = identifier or wallet.defaultId
        # TODO: Should not raise an error but should be able to choose a signer
        assert identifier, "Client has multiple signers, cannot choose one"
        node.clientAuthNr.addClient(identifier, wallet.getVerkey(identifier))

    def clientExists(self, clientName):
        return clientName in self.clients

    def printMsgForUnknownClient(self):
        self.print("No such client. See: 'help new' for more details")

    def printMsgForUnknownWallet(self, walletName):
        self.print("No such wallet {}.".format(walletName))

    def sendMsg(self, clientName, msg):
        client = self.clients.get(clientName, None)
        wallet = self.wallets.get(clientName, None)  # type: Wallet
        if client:
            if wallet:
                req = wallet.signOp(msg)
                request, = client.submitReqs(req)
                self.requests[str(request.reqId)] = request.reqId
            else:
                self._newWallet(clientName)
                self.printNoKeyMsg()
        else:
            self.printMsgForUnknownClient()

    def getReply(self, clientName, reqId):
        client = self.clients.get(clientName, None)
        requestID = self.requests.get(reqId, None)
        if client and requestID:
            reply, status = client.getReply(requestID)
            self.print("Reply for the request: {}".format(reply))
            self.print("Status: {}".format(status))
        elif not client:
            self.printMsgForUnknownClient()
        else:
            self.print("No such request. See: 'help new' for more details")

    def showDetails(self, clientName, reqId):
        client = self.clients.get(clientName, None)
        requestID = self.requests.get(reqId, None)
        if client and requestID:
            client.showReplyDetails(requestID)
        else:
            self.printMsgForUnknownClient()

    async def shell(self, *commands, interactive=True):
        """
        Coroutine that runs command, including those from an interactive
        command line.

        :param commands: an iterable of commands to run first
        :param interactive: when True, this coroutine will process commands
            entered on the command line.
        :return:
        """
        # First handle any commands passed in
        for command in commands:
            if not command.startswith("--"):
                self.print("\nRunning command: '{}'...\n".format(command))
                self.parse(command)

        # then handle commands from the prompt
        while interactive:
            try:
                result = await self.cli.run_async()
                cmd = result.text if result else ""
                cmds = cmd.strip().splitlines()
                for c in cmds:
                    self.parse(c)
            except (EOFError, KeyboardInterrupt, Exit):
                break

        self.print('Goodbye.')

    def _simpleAction(self, matchedVars):
        if matchedVars.get('simple'):
            cmd = matchedVars.get('simple')
            if cmd == 'status':
                self.getStatus()
            elif cmd == 'license':
                self.printCmdHelper('license')
            elif cmd in ['exit', 'quit']:
                raise Exit
            return True

    def _helpAction(self, matchedVars):
        if matchedVars.get('command') == 'help':
            helpable = matchedVars.get('helpable')
            node_or_cli = matchedVars.get('node_or_cli')
            if helpable:
                if node_or_cli:
                    self.printCmdHelper(command="{}{}".
                                        format(helpable, node_or_cli))
                else:
                    self.printCmdHelper(command=helpable)
            else:
                self.printHelp()
            return True

    def _listAction(self, matchedVars):
        if matchedVars.get('command') == 'list':
            for cmd in self.commands:
                self.print(cmd)
            return True

    def _newNodeAction(self, matchedVars):
        if matchedVars.get('node_command') == 'new':
            self.createEntities('node_name', 'more_nodes',
                                matchedVars, self.newNode)
            return True

    def _newClientAction(self, matchedVars):
        if matchedVars.get('client_command') == 'new':
            self.createEntities('client_name', 'more_clients',
                                matchedVars, self.newClient)
            return True

    def _statusNodeAction(self, matchedVars):
        if matchedVars.get('node_command') == 'status':
            node = matchedVars.get('node_name')
            self.statusNode(node)
            return True

    def _statusClientAction(self, matchedVars):
        if matchedVars.get('client_command') == 'status':
            client = matchedVars.get('client_name')
            self.statusClient(client)
            return True

    def _keyShareAction(self, matchedVars):
        if matchedVars.get('node_command') == 'keyshare':
            name = matchedVars.get('node_name')
            self.keyshare(name)
            return True

    def _clientCommand(self, matchedVars):
        if matchedVars.get('client') == 'client':
            client_name = matchedVars.get('client_name')
            client_action = matchedVars.get('cli_action')
            if client_action == 'send':
                msg = matchedVars.get('msg')
                try:
                    actualMsgRepr = ast.literal_eval(msg)
                except Exception as ex:
                    self.print("error evaluating msg expression: {}".
                               format(ex), Token.BoldOrange)
                    return True
                self.sendMsg(client_name, actualMsgRepr)
                return True
            elif client_action == 'show':
                req_id = matchedVars.get('req_id')
                self.getReply(client_name, req_id)
                return True

    def _loadPluginDirAction(self, matchedVars):
        if matchedVars.get('load_plugins') == 'load plugins from':
            pluginsPath = matchedVars.get('plugin_dir')
            try:
                plugins = PluginLoader(
                    pluginsPath).plugins  # type: Dict[str, Set]
                for pluginSet in plugins.values():
                    for plugin in pluginSet:
                        if hasattr(plugin, "supportsCli") and plugin.supportsCli:
                            plugin.cli = self
                            parserReInitNeeded = False
                            if hasattr(plugin, "grams") and \
                                    isinstance(plugin.grams,
                                               list) and plugin.grams:
                                self._allGrams.append(plugin.grams)
                                parserReInitNeeded = True
                            # TODO Need to check if `plugin.cliActionNames`
                            #  conflicts with any of `self.cliActions`
                            if hasattr(plugin, "cliActionNames") and \
                                    isinstance(plugin.cliActionNames, set) and \
                                    plugin.cliActionNames:
                                self.cliActions.update(plugin.cliActionNames)
                                # TODO: Find better way to reinitialize completers
                                # , also might need to reinitialize lexers
                                self._completers = {}
                                parserReInitNeeded = True
                            if parserReInitNeeded:
                                self.initializeInputParser()
                                self.cli.application.buffer.completer = \
                                    self.grammarCompleter
                                self.cli.application.layout.children[
                                    1].children[
                                    1].content.content.lexer = self.grammarLexer
                            if hasattr(plugin, "actions") and \
                                    isinstance(plugin.actions, list):
                                self._actions.extend(plugin.actions)

                self.plugins.update(plugins)
                self.pluginPaths.append(pluginsPath)
            except FileNotFoundError as ex:
                _, err = ex.args
                self.print(err, Token.BoldOrange)
            return True

    def _addKeyAction(self, matchedVars):
        if matchedVars.get('add_key') == 'add key':
            verkey = matchedVars.get('verkey')
            # TODO make verkey case insensitive
            identifier = matchedVars.get('identifier')
            if identifier in self.externalClientKeys:
                self.print("identifier already added", Token.Error)
                return
            self.externalClientKeys[identifier] = verkey
            for n in self.nodes.values():
                n.clientAuthNr.addClient(identifier, verkey)
            return True

    def _addSignerToGivenWallet(self, signer, wallet: Wallet=None,
                                showMsg: bool=False):
        if not wallet:
            wallet = self._newWallet()
        wallet.addSigner(signer=signer)
        if showMsg:
            self.print("Key created in keyring " + wallet.name)

    # def _addSignerToWallet(self, signer, wallet=None):
    #     self._addSignerToGivenWallet(signer, wallet)
    #     self.print("Key created in keyring " + wallet.name)

    def _newSigner(self,
                   wallet=None,
                   identifier=None,
                   seed=None,
                   alias=None):

        cseed = cleanSeed(seed)

        signer = SimpleSigner(identifier=identifier, seed=cseed, alias=alias)
        self._addSignerToGivenWallet(signer, wallet, showMsg=True)
        self.print("Identifier for key is {}".format(signer.identifier))
        if alias:
            self.print("Alias for identifier is {}".format(signer.alias))
        self._setActiveIdentifier(signer.identifier)
        self.bootstrapClientKeys(signer.identifier,
                            signer.verkey,
                            self.nodes.values())
        return signer

    @staticmethod
    def bootstrapClientKeys(idr, verkey, nodes):
        bootstrapClientKeys(idr, verkey, nodes)

    def _newKeyAction(self, matchedVars):
        if matchedVars.get('new_key') == 'new key':
            seed = matchedVars.get('seed')
            alias = matchedVars.get('alias')
            self._newSigner(seed=seed, alias=alias, wallet=self.activeWallet)
            return True

    def _buildWalletClass(self, nm):
        return self.walletClass(nm)

    @property
    def walletClass(self):
        return Wallet

    def _newWallet(self, walletName=None):
        nm = walletName or self.defaultWalletName
        if nm in self.wallets:
            self.print("Keyring {} already exists".format(nm))
            wallet = self._wallets[nm]
            self.activeWallet = wallet  # type: Wallet
            return wallet
        wallet = self._buildWalletClass(nm)
        self._wallets[nm] = wallet
        self.print("New keyring {} created".format(nm))
        self.activeWallet = wallet
        # TODO when the command is implemented
        # if nm == self.defaultWalletName:
        #     self.print("Note, you can rename this wallet by:")
        #     self.print("    rename wallet {} to NewName".format(nm))
        return wallet

    def _listIdsAction(self, matchedVars):
        if matchedVars.get('list_ids') == 'list ids':
            self.print("Active keyring: {}".format(self.activeWallet.name))
            self.print('\n'.join(self.activeWallet.listIds()))
            return True

    def _checkIfIdentifierConflicts(self, name, checkInWallets=True,
                                    checkInAliases=True, checkInSigners=True):
        allAliases = []
        allSigners = []
        allWallets = []

        for wk, wv in self.wallets.items():
            if checkInAliases:
                allAliases.extend(list(wv.aliasesToIds.keys()))
            if checkInSigners:
                allSigners.extend(list(wv.listIds()))
            if checkInWallets:
                allWallets.append(wk)

        if name:
            if name in allWallets:
                self.print(
                    "{} conflicts with an existing keyring name. "
                    "Please choose a new name".format(name), Token.Warning)
                return True
            if name in allAliases:
                self.print(
                    "{} conflicts with an existing alias. "
                    "Please choose a new name".format(name), Token.Warning)
                return True
            if name in allSigners:
                self.print(
                    "{} conflicts with an existing identifier. "
                    "Please choose a new name".format(name), Token.Warning)
                return True
            return False
        else:
            return False

    def _searchAndSetWallet(self, name):
        wallet = self.wallets.get(name)
        if wallet:
            self.activeWallet = wallet
            self.print("Current keyring set to {}".format(name))
        else:
            self.print("No such keyring found")
        return True

    def _useKeyringAction(self, matchedVars):
        if matchedVars.get('use_kr') == 'use keyring':
            name = matchedVars.get('keyring')
            self._searchAndSetWallet(name)
            return True

    def _setActiveIdentifier(self, idrOrAlias):
        if self.activeWallet:
            wallet = self.activeWallet
            if idrOrAlias not in wallet.aliasesToIds and idrOrAlias not in wallet.idsToSigners:
                return False
            idrFromAlias = wallet.aliasesToIds.get(idrOrAlias)
            # If alias found
            if idrFromAlias:
                self.activeIdentifier = idrFromAlias
                self.activeAlias = idrOrAlias
            else:
                alias = [k for k, v
                         in wallet.aliasesToIds.items()
                         if v == idrOrAlias]
                self.activeAlias = alias[0] if alias else None
                self.activeIdentifier = idrOrAlias
            self.print("Current identifier set to {}".
                       format(self.activeAlias or self.activeIdentifier))
            return True
        return False

    def _useIdentifierAction(self, matchedVars):
        if matchedVars.get('use_id') == 'use identifier':
            nymOrAlias = matchedVars.get('identifier')
            found = self._setActiveIdentifier(nymOrAlias)
            if not found:
                self.print("No such identifier found in current keyring")
            return True

    def _setPrompt(self, promptText):
        app = create_prompt_application('{}> '.format(promptText),
                                        lexer=self.grammarLexer,
                                        completer=self.grammarCompleter,
                                        style=self.style,
                                        history=self.pers_hist)
        self.cli.application = app
        self.currPromptText = promptText
        # getTokens = lambda _: [(Token.Prompt, promptText + "> ")]
        # self.cli.application.layout.children[1].children[0]\
        #     .content.content.get_tokens = getTokens

    def parse(self, cmdText):
        cmdText = cmdText.strip()
        m = self.grammar.match(cmdText)
        # noinspection PyProtectedMember
        if m and len(m.variables()._tuples):
            matchedVars = m.variables()
            self.logger.info("CLI command entered: {}".format(cmdText),
                             extra={"cli": False})
            for action in self.actions:
                r = action(matchedVars)
                if r:
                    break
            else:
                self.invalidCmd(cmdText)
        else:
            if cmdText != "":
                self.invalidCmd(cmdText)

    @staticmethod
    def createEntities(name: str, moreNames: str, matchedVars, initializer):
        entity = matchedVars.get(name)
        more = matchedVars.get(moreNames)
        more = more.split(',') if more is not None and len(more) > 0 else []
        names = [n for n in [entity] + more if len(n) != 0]
        seed = matchedVars.get("seed")
        identifier = matchedVars.get("nym")
        if len(names) == 1 and (seed or identifier):
            initializer(names[0].strip(), seed=seed, identifier=identifier)
        else:
            for name in names:
                initializer(name.strip())

    def invalidCmd(self, cmdText):
        self.print("Invalid command: '{}'\n".format(cmdText))
        self.printCmdHelper(command=None)

    def nextAvailableClientAddr(self, curClientPort=8100):
        self.curClientPort = self.curClientPort or curClientPort
        # TODO: Find a better way to do this
        self.curClientPort += random.randint(1, 200)
        host = "0.0.0.0"
        try:
            checkPortAvailable((host, self.curClientPort))
            return host, self.curClientPort
        except Exception as ex:
            tokens = [(Token.Error, "Cannot bind to port {}: {}, "
                                    "trying another port.".
                       format(self.curClientPort, ex))]
            self.printTokens(tokens)
            return self.nextAvailableClientAddr(self.curClientPort)

    @property
    def hasAnyKey(self):
        if not self.activeWallet.defaultId:
            self.printNoKeyMsg()
            return False
        return True

    def printNoKeyMsg(self):
        self.print("No key present in keyring")
        self.printUsage(("new key", ))

    def printUsage(self, msgs):
        self.print("\nUsage:")
        for m in msgs:
            self.print('  {}'.format(m))
        self.print("\n")

    # TODO: Do we keep this? What happens when we allow the CLI to connect
    # to remote nodes?
    def cleanUp(self):
        dataPath = os.path.join(self.config.baseDir, "data")
        try:
            shutil.rmtree(dataPath)
        except FileNotFoundError:
            pass

        client = pyorient.OrientDB(self.config.OrientDB["host"],
                                   self.config.OrientDB["port"])
        user = self.config.OrientDB["user"]
        password = self.config.OrientDB["password"]
        client.connect(user, password)

        def dropdbs():
            i = 0
            names = [n for n in
                     client.db_list().oRecordData['databases'].keys()]
            for nm in names:
                try:
                    client.db_drop(nm)
                    i += 1
                except:
                    continue
            return i

        dropdbs()


class Exit(Exception):
    pass
