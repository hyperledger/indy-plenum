from __future__ import unicode_literals

# noinspection PyUnresolvedReferences
import plenum.cli.ensure_logging_not_setup

from typing import Dict

import re
from prompt_toolkit.utils import is_windows, is_conemu_ansi

from plenum.client.signer import SimpleSigner

if is_windows():
    from prompt_toolkit.terminal.win32_output import Win32Output
    from prompt_toolkit.terminal.conemu_output import ConEmuOutput
else:
    from prompt_toolkit.terminal.vt100_output import Vt100_Output

import configparser
import os
from configparser import ConfigParser

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
from plenum.common.util import setupLogging, getlogger, CliHandler, \
    TRACE_LOG_LEVEL, getMaxFailures, checkPortAvailable
from plenum.server.node import Node
from plenum.common.types import CLIENT_STACK_SUFFIX
from plenum.server.plugin_loader import PluginLoader
from plenum.server.replica import Replica
from collections import OrderedDict


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

    # noinspection PyPep8
    def __init__(self, looper, basedirpath, nodeReg, cliNodeReg, output=None, debug=False,
                 logFileName=None):
        self.curClientPort = None
        logging.root.addHandler(CliHandler(self.out))

        self.looper = looper
        self.basedirpath = basedirpath
        self.nodeReg = nodeReg
        self.cliNodeReg = cliNodeReg

        # Used to store created clients
        self.clients = {}  # clientName -> Client
        # To store the created requests
        self.requests = {}
        # To store the nodes created
        self.nodes = {}
        self.externalClientKeys = {}  # type: Dict[str,str]

        self.cliCmds = {'status', 'new'}
        self.nodeCmds = self.cliCmds | {'keyshare'}
        self.helpablesCommands = self.cliCmds | self.nodeCmds
        self.simpleCmds = {'status', 'exit', 'quit', 'license'}
        self.commands = {'list', 'help'} | self.simpleCmds
        self.cliActions = {'send', 'show'}
        self.commands.update(self.cliCmds)
        self.commands.update(self.nodeCmds)
        self.node_or_cli = ['node',  'client']
        self.nodeNames = list(self.nodeReg.keys()) + ["all"]
        self.debug = debug
        self.plugins = {}
        '''
        examples:
        status

        new node Alpha
        new node all
        new client Joe
        client Joe send <msg>
        client Joe show 1
        '''

        psep = re.escape(os.path.sep)

        self.utilGrams = [
            "(\s* (?P<simple>{}) \s*) |".format(self.relist(self.simpleCmds)),
            "(\s* (?P<load>load) \s+ (?P<file_name>[.a-zA-z0-9{}]+) \s*) |".format(psep),
            "(\s* (?P<command>help) (\s+ (?P<helpable>[a-zA-Z0-9]+) )? (\s+ (?P<node_or_cli>{}) )?\s*) |".format(self.relist(self.node_or_cli)),
            "(\s* (?P<command>list) \s*)"
        ]

        self.nodeGrams = [
            "(\s* (?P<node_command>{}) \s+ (?P<node_or_cli>nodes?)   \s+ (?P<node_name>[a-zA-Z0-9]+)\s*) |".format(self.relist(self.nodeCmds)),
            "(\s* (?P<load_plugins>load\s+plugins\s+from) \s+ (?P<plugin_dir>[a-zA-Z0-9-:{}]+) \s*)".format(psep),
        ]

        self.clientGrams = [
            "(\s* (?P<client_command>{}) \s+ (?P<node_or_cli>clients?)   \s+ (?P<client_name>[a-zA-Z0-9]+) \s*) |".format(self.relist(self.cliCmds)),
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>send) \s+ (?P<msg>\{\s*.*\})  \s*)  |",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>show) \s+ (?P<req_id>[0-9]+)  \s*)  |",
            "(\s* (?P<add_key>add\s+key) \s+ (?P<verkey>[a-fA-F0-9]+) \s+ (?P<for_client>for\s+client) \s+ (?P<identifier>[a-zA-Z0-9]+) \s*)",
        ]

        self.lexers = {
            'node_command': SimpleLexer(Token.Keyword),
            'command': SimpleLexer(Token.Keyword),
            'helpable': SimpleLexer(Token.Keyword),
            'load_plugins': SimpleLexer(Token.Keyword),
            'load': SimpleLexer(Token.Keyword),
            'node_or_cli': SimpleLexer(Token.Keyword),
            'arg1': SimpleLexer(Token.Name),
            'node_name': SimpleLexer(Token.Name),
            'more_nodes': SimpleLexer(Token.Name),
            'simple': SimpleLexer(Token.Keyword),
            'client_command': SimpleLexer(Token.Keyword),
            'add_key': SimpleLexer(Token.Keyword),
            'verkey': SimpleLexer(Token.Literal),
            'for_client': SimpleLexer(Token.Keyword),
            'identifier': SimpleLexer(Token.Name),
        }

        self.clientWC = WordCompleter([])

        self.completers = {
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
            'cli_action': WordCompleter(self.cliActions),
            'simple': WordCompleter(self.simpleCmds),
            'add_key': WordCompleter(['add key']),
            'for_client': WordCompleter(['for client']),
        }

        self.initializeGrammar()

        self.initializeGrammarLexer()

        self.initializeGrammarCompleter()

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

        pers_hist = FileHistory('.{}-cli-history'.format(self.name))

        # Create interface.
        app = create_prompt_application('{}> '.format(self.name),
                                        lexer=self.grammarLexer,
                                        completer=self.grammarCompleter,
                                        style=self.style,
                                        history=pers_hist)

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

        # Patch stdout in something that will always print *above* the prompt
        # when something is written to stdout.
        sys.stdout = self.cli.stdout_proxy()
        setupLogging(TRACE_LOG_LEVEL,
                     Console.Wordage.mute,
                     filename=logFileName)

        self.logger = getlogger("cli")
        self.print("\n{}-CLI (c) 2016 Evernym, Inc.".format(self.properName))
        self.print("Node registry loaded.")
        self.print("None of these are created or running yet.")

        self.showNodeRegistry()
        self.print("Type 'help' for more information.")

    @staticmethod
    def relist(seq):
        return '(' + '|'.join(seq) + ')'

    def initializeGrammar(self):
        self.utilGrams[-1] += " |"
        self.nodeGrams[-1] += " |"
        self.grams = self.utilGrams + self.nodeGrams + self.clientGrams
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
        self.print("""{}-CLI, a simple command-line interface for a
        {} sandbox.
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
        for n, (ip, port) in self.nodeReg.items():
            t.append((Token.Name, "    " + n))
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
        opVerifiers = self.plugins['VERIFICATION'] if self.plugins else []
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
                                  self.nodeReg,
                                  basedirpath=self.basedirpath,
                                  opVerifiers=opVerifiers)
            self.nodes[name] = node
            self.looper.add(node)
            node.startKeySharing()
            for client in self.clients.values():
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
            if client._conns:
                self.printNames(client._conns, newline=True)
            else:
                self.printVoid()
            self.print("    Identifier: {}".format(client.defaultIdentifier))
            self.print("    Verification key: {}".format(client.getSigner().verkey))
            self.print("    Submissions: {}".format(client.lastReqId))

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
            nha = "{}:{}".format(*self.nodeReg.get(nodeName))
            self.print("    Node listener: " + nha)
            cha = "{}:{}".format(
                *self.cliNodeReg.get(nodeName + CLIENT_STACK_SUFFIX))
            self.print("    Client listener: " + cha)
            self.print("    Status: {}".format(node.status.name))
            self.print('    Connections: ', newline=False)
            connecteds = node.nodestack.connecteds()
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
            clients = node.clientstack.connecteds()
            if clients:
                self.printNames(clients, newline=True)
            else:
                self.printVoid()

    def newClient(self, clientName, seed=None, identifier=None, signer=None):
        try:
            self.ensureValidClientId(clientName)
            client_addr = self.nextAvailableClientAddr()
            if not signer:
                seed = seed.encode("utf-8") if seed else None
                signer = SimpleSigner(identifier=identifier, seed=seed) \
                    if (seed or identifier) else None
            client = self.ClientClass(clientName,
                                      ha=client_addr,
                                      nodeReg=self.cliNodeReg,
                                      signer=signer,
                                      basedirpath=self.basedirpath)
            self.looper.add(client)
            for node in self.nodes.values():
                self.bootstrapClientKey(client, node)
            self.clients[clientName] = client
            self.clientWC.words = list(self.clients.keys())
            return client
        except ValueError as ve:
            self.print(ve.args[0], Token.Error)

    @staticmethod
    def bootstrapClientKey(client, node):
        idAndKey = client.getSigner().identifier, client.getSigner().verkey
        node.clientAuthNr.addClient(*idAndKey)

    def sendMsg(self, clientName, msg):
        client = self.clients.get(clientName, None)
        if client:
            request, = client.submit(msg)
            self.requests[str(request.reqId)] = request.reqId
        else:
            self.print("No such client. See: 'help new' for more details")

    def getReply(self, clientName, reqId):
        client = self.clients.get(clientName, None)
        requestID = self.requests.get(reqId, None)
        if client and requestID:
            reply, status = client.getReply(requestID)
            self.print("Reply for the request: {}".format(reply))
            self.print("Status: {}".format(status))
        elif not client:
            self.print("No such client. See: 'help new' for more details")
        else:
            self.print("No such request. See: 'help new' for more details")

    def showDetails(self, clientName, reqId):
        client = self.clients.get(clientName, None)
        requestID = self.requests.get(reqId, None)
        if client and requestID:
            client.showReplyDetails(requestID)
        else:
            self.print("No such client. See: 'help new' for more details")

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
            self.print("\nRunning command: '{}'...\n".format(command))
            self.parse(command)

        # then handle commands from the prompt
        while interactive:
            try:
                result = await self.cli.run_async()
                self.parse(result.text if result else "")
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
            # else:
            #     self.printCmdHelper("sendmsg")

    def _loadPluginDirAction(self, matchedVars):
        if matchedVars.get('load_plugins') == 'load plugins from':
            pluginsPath = matchedVars.get('plugin_dir')
            try:
                self.plugins = PluginLoader(pluginsPath).plugins
            except FileNotFoundError as ex:
                _, err = ex.args
                self.print(err, Token.BoldOrange)
            return True

    def _loadPluginAction(self, matchedVars):
        if matchedVars.get('load') == 'load':
            file = matchedVars.get("file_name")
            if os.path.exists(file):
                try:
                    self.loadFromFile(file)
                    self.print("Node registry loaded.")
                    self.showNodeRegistry()
                except configparser.ParsingError:
                    self.logger.warn("Could not parse file. "
                                     "Please ensure that the file "
                                     "has sections node_reg "
                                     "and client_node_reg.",
                                     extra={'cli': 'WARNING'})
            else:
                self.logger.warn("File {} not found.".format(file),
                                 extra={"cli": "WARNING"})
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

    def getActionList(self):
        return [self._simpleAction, self._helpAction, self._listAction,
                self._newNodeAction, self._newClientAction,
                self._statusNodeAction, self._statusClientAction,
                self._keyShareAction, self._loadPluginDirAction,
                self._clientCommand, self._loadPluginAction, self._addKeyAction]

    def parse(self, cmdText):
        m = self.grammar.match(cmdText)
        if m:
            matchedVars = m.variables()
            self.logger.info("CLI command entered: {}".format(cmdText),
                             extra={"cli": False})
            for action in self.getActionList():
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
        self.curClientPort += 1
        host = "127.0.0.1"
        try:
            checkPortAvailable((host,self.curClientPort))
            return host, self.curClientPort
        except Exception as ex:
            tokens = [(Token.Error, "Cannot bind to port {}: {}, "
                                    "trying another port.".format(
                self.curClientPort, ex))]
            self.printTokens(tokens)
            return self.nextAvailableClientAddr(self.curClientPort)


class Exit(Exception):
    pass
