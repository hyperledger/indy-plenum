from __future__ import unicode_literals

# noinspection PyUnresolvedReferences
import configparser
import os
from configparser import ConfigParser
import zeno.cli.ensure_logging_not_setup

import time

from prompt_toolkit.history import FileHistory

from functools import reduce, partial
import logging
import sys
from collections import defaultdict

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
from zeno.client.client import Client
from zeno.common.util import setupLogging, getlogger, CliHandler, \
    TRACE_LOG_LEVEL
from zeno.server.node import Node
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

    # noinspection PyPep8
    def __init__(self, looper, tmpdir, nodeReg, cliNodeReg):
        self.curClientPort = None
        logging.root.addHandler(CliHandler(self.out))

        self.looper = looper
        self.tmpdir = tmpdir
        self.nodeReg = nodeReg
        self.cliNodeReg = cliNodeReg

        # Used to store created clients
        self.clients = {}  # clientId -> Client
        # To store the created requests
        self.requests = {}
        # To store the nodes created
        self.nodes = {}

        self.cliCmds = {'new', 'status', 'list'}
        self.nodeCmds = {'new', 'status', 'list', 'keyshare'}
        self.helpablesCommands = self.cliCmds | self.nodeCmds
        self.simpleCmds = {'status', 'help', 'exit',
                           'quit',
                           'license'}
        self.commands = {'list'} | self.simpleCmds
        self.cliActions = {'send', 'show'}
        self.commands.update(self.cliCmds)
        self.commands.update(self.nodeCmds)
        self.node_or_cli = ['node', 'client']
        self.nodeNames = list(self.nodeReg.keys()) + ["all"]

        '''
        examples:
        status

        new node Alpha
        new node all
        new client Joe
        client Joe send <msg>
        client Joe show 1
        '''

        def re(seq):
            return '(' + '|'.join(seq) + ')'

        grams = [
            "(\s* (?P<simple>{}) \s*) |".format(re(self.simpleCmds)),
            "(\s* (?P<command>{}) (\s+ (?P<helpable>[a-zA-Z0-9]+) )?\s*) "
            "|".format(re(self.commands)),
            "(\s* (?P<client_command>{}) \s+ (?P<node_or_cli>clients?)   \s+ (?P<client_name>[a-zA-Z0-9]+) \s*) |"
                .format(re(self.cliCmds)),
            "(\s* (?P<node_command>{}) \s+ (?P<node_or_cli>nodes?)   \s+ (?P<node_name>[a-zA-Z0-9]+) \s*) |"
                .format(re(self.nodeCmds)),
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>send) \s+ (?P<msg>\{\s*\".*\})  \s*)  |",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>show) \s+ (?P<req_id>[0-9]+)  \s*)  |",
            "(\s* (?P<load>load) \s+ (?P<file_name>[.a-zA-z0-9{}]+) \s*)".format(os.path.sep)
            ]

        self.grammar = compile("".join(grams))

        lexer = GrammarLexer(self.grammar, lexers={
            'node_command': SimpleLexer(Token.Keyword),
            'command': SimpleLexer(Token.Keyword),
            'helpable': SimpleLexer(Token.Keyword),
            'load': SimpleLexer(Token.Keyword),
            'node_or_cli': SimpleLexer(Token.Keyword),
            'arg2': SimpleLexer(Token.Name),
            'node_name': SimpleLexer(Token.Name),
            'simple': SimpleLexer(Token.Keyword),
            'client_command': SimpleLexer(Token.Keyword),
        })

        self.clientWC = WordCompleter([])

        completer = GrammarCompleter(self.grammar, {
            'node_command': WordCompleter(self.nodeCmds),
            'client_command': WordCompleter(self.cliCmds),
            'client': WordCompleter(['client']),
            'command': WordCompleter(self.commands),
            'node_or_cli': WordCompleter(self.node_or_cli),
            'node_name': WordCompleter(self.nodeNames),
            'helpable': WordCompleter(self.helpablesCommands),
            'load': WordCompleter('load'),
            'client_name': self.clientWC,
            'cli_action': WordCompleter(self.cliActions),
            'simple': WordCompleter(self.simpleCmds)
        })

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

        # Create an asyncio `EventLoop` object. This is a wrapper around the
        # asyncio loop that can be passed into prompt_toolkit.
        eventloop = create_asyncio_eventloop(looper.loop)

        pers_hist = FileHistory('.zeno-cli-history')

        # Create interface.
        app = create_prompt_application('zeno> ',
                                        lexer=lexer,
                                        completer=completer,
                                        style=self.style,
                                        history=pers_hist)
        self.cli = CommandLineInterface(
                application=app,
                eventloop=eventloop,
                output=CustomOutput.from_pty(sys.__stdout__, true_color=True))

        # Patch stdout in something that will always print *above* the prompt
        # when something is written to stdout.
        sys.stdout = self.cli.stdout_proxy()
        setupLogging(TRACE_LOG_LEVEL,
                     Console.Wordage.mute,
                     filename="log/cli.log")
        self.logger = getlogger("cli")
        print("\nzeno-CLI (c) 2016 Evernym, Inc.")
        print("Node registry loaded.")
        print("None of these are created or running yet.")

        self.showNodeRegistry()
        print("Type 'help' for more information.")

    def createFunctionMappings(self):

        def newHelper():
            print("""Is used to create a new node or a client.
                     Usage: new <node/client> <nodeName/clientName>""")

        def statusHelper():
            print("status command helper")

        def clientHelper():
            print("Can be used to create a new client")

        def listHelper():
            print("List all the commands, you can use in this CLI.")

        def exitHelper():
            print("Exits the CLI")

        def licenseHelper():
            print("""
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

        def sendmsgHelper():
            print("""Used to send a message from a client to nodes"
                     Usage: sendmsg <clientName> <{Message}>""")

        def getreplyHelper():
            print("""Used to send a message from a client to nodes"
                     Usage: getreply <clientName> <reqID>""")

        def showdetailsHelper():
            print("""Used to send a message from a client to nodes"
                     Usage: showdetails <clientName> <reqID>""")

        def defaultHelper():
            self.printHelp()

        mappings = {
            'new': newHelper,
            'status': statusHelper,
            'list': listHelper,
            'client': clientHelper,
            'license': licenseHelper,
            'sendmsg': sendmsgHelper,
            'getreply': getreplyHelper,
            'showdetails': showdetailsHelper,
            'exit': exitHelper
        }

        return defaultdict(lambda: defaultHelper, **mappings)

    def print(self, msg, token=None, newline=True):
        if newline:
            msg += "\n"
        part = partial(self.cli.print_tokens, [(token, msg)])
        self.cli.run_in_terminal(part)

    def out(self, record, extra_cli_value=None):
        """
        Callback so that this cli can manage colors

        :param record: a log record served up from a custom handler
        :param extra_cli_value: the "cli" value in the extra dictionary
        :return:
        """

        # self.trackElectionStarted(record)
        # self.trackElectionCompleted(record)
        # TODO come up with an encoding scheme
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


    @staticmethod
    def printHelp():
        print("""zeno-CLI, a simple command-line interface for an zeno protocol sandbox.
Commands:
    help - Shows this help message
    help <command> - Shows the help message of <command>
    new - creates a new node or client
    keyshare - manually starts key sharing of a node
    status - Shows general status of the sandbox
    status <node_name>|<client_name> - Shows specific status
    list - Shows the list of commands you can run
    license - Show the license
    exit - exit the command-line interface ('quit' also works)""")

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
        return cls._loadRegistry(cfg, 'client_node_reg')

    @classmethod
    def _loadRegistry(cls, cfg: ConfigParser, reg: str):
        registry = OrderedDict()
        for n in cfg.items(reg):
            host, port = n[1].split()
            registry.update({n[0].capitalize(): (host, int(port))})
        return registry

    def getStatus(self):
        if len(self.nodes) > 1:
            print("The following nodes are up and running: ")
        elif len(self.nodes) == 1:
            print("The following node is up and running: ")
        else:
            print("No nodes are running. Try typing 'new node <name>'.")
        for node in self.nodes:
            print(node)
        if len(self.nodes) > 1:
            print("The following clients are up and running: ")
        elif len(self.nodes) == 1:
            print("The following client is up and running: ")
        else:
            print("No clients are running. Try typing 'new client <name>'.")
        for client in self.clients:
            print(client)

    def keyshare(self, nodeName):
        node = self.nodes.get(nodeName, None)
        if node is not None:
            node = self.nodes[nodeName]
            node.startKeySharing()
        elif nodeName not in self.nodeReg:
            tokens = [(Token.Error, "Invalid node name '{}'. ".format(nodeName))]
            self.printTokens(tokens)
            self.showValidNodes()
            return
        else:
            tokens = [(Token.Error, "Node '{}' not started. ".format(nodeName))]
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

    def newNode(self, nodeName):
        if nodeName in self.nodes:
            print("Node {} already exists.\n".format(nodeName))
            return
        if nodeName == "all":
            names = set(self.nodeReg.keys()) - set(self.nodes.keys())
        elif nodeName not in self.nodeReg:
            tokens = [(Token.Error, "Invalid node name '{}'. ".format(nodeName))]
            self.printTokens(tokens)
            self.showValidNodes()
            return
        else:
            names = [nodeName]
        for name in names:
            node = Node(name, self.nodeReg, basedirpath=self.tmpdir)
            self.nodes[name] = node
            self.looper.add(node)
            node.startKeySharing()
            for client in self.clients.values():
                self.bootstrapClientKey(client, node)

    def ensureValidClientId(self, clientId):
        """
        Ensures client id is not already used or is not starting with node
        names.

        :param clientId:
        :return:
        """
        if clientId in self.clients:
            raise ValueError("Client {} already exists.\n".format(clientId))

        if any([clientId.startswith(nm) for nm in self.nodeNames]):
            raise ValueError("Client name cannot start with node names, "
                             "which are {}\n"
                             .format(', '.join(self.nodeReg.keys())))

    def statusClient(self, clientId):
        if clientId == "all":
            for nm in self.clients:
                self.statusClient(nm)
            return
        if clientId not in self.clients:
            self.print("client not found", Token.Error)
        else:
            client = self.clients[clientId]  # type: Client

            self.printTokens([(Token.Heading, 'Status for client:'),
                              (Token.Name, client.name)],
                             separator=' ', end='\n')
            self.print("    age (seconds): {:.0f}".
                       format(time.perf_counter() - client.created))
            self.print("    connected to: ", newline=False)
            if client._conns:
                self.printNames(client._conns, newline=True)
            else:
                self.print("<none>")
            self.print("    identifier: {}".format(client.signer.identifier))
            self.print("    verification key: {}".format(client. signer.verkey))
            self.print("    submissions: {}".format(client.lastReqId))

    def statusNode(self, nodeName):
        if nodeName == "all":
            for nm in self.nodes:
                self.statusNode(nm)
            return
        if nodeName not in self.nodes:
            self.print("node not found", Token.Error)
        else:
            node = self.nodes[nodeName]  # type: Node

            self.printTokens([(Token.Heading, 'Status for node:'),
                              (Token.Name, node.name)],
                             separator=' ', end='\n')
            self.print("    status: {}".format(node.status.name))
            self.print("    age (seconds): {:.0f}".
                       format(time.perf_counter() - node.created))
            self.print("    connected nodes: ", newline=False)
            if node._conns:
                self.printNames(node._conns, newline=True)
            else:
                self.print("<none>")
            self.print("    connected clients: ", newline=False)
            clis = node.clientstack.connecteds()
            if clis:
                self.printNames(clis, newline=True)
            else:
                self.print("<none>")
            self.print("    client verification keys: {}".
                       format(node.clientAuthNr.clients))

    def newClient(self, clientId):
        try:
            self.ensureValidClientId(clientId)
            client_addr = self.getNextAvailableAddr()

            client = Client(clientId,
                            ha=client_addr,
                            nodeReg=self.cliNodeReg,
                            basedirpath=self.tmpdir)
            self.looper.add(client)
            for node in self.nodes.values():
                self.bootstrapClientKey(client, node)
            self.clients[clientId] = client
            self.clientWC.words = list(self.clients.keys())
        except ValueError as ve:
            self.print(ve.args[0], Token.Error)

    @staticmethod
    def bootstrapClientKey(client, node):
        idAndKey = client.signer.identifier, client.signer.verkey
        node.clientAuthNr.addClient(*idAndKey)

    def sendMsg(self, clientName, msg):
        client = self.clients.get(clientName, None)
        if client:
            request, = client.submit(msg)
            self.requests[str(request.reqId)] = request.reqId
        else:
            print("No such client. See: 'help new' for more details")

    def getReply(self, clientName, reqId):
        client = self.clients.get(clientName, None)
        requestID = self.requests.get(reqId, None)
        if client and requestID:
            reply, status = client.getReply(requestID)
            print("Reply for the request: {}\n".format(reply))
            print("Status: {}\n".format(status))
        elif not client:
            print("No such client. See: 'help new' for more details")
        else:
            print("No such request. See: 'help new' for more details")

    def showDetails(self, clientName, reqId):
        client = self.clients.get(clientName, None)
        requestID = self.requests.get(reqId, None)
        if client and requestID:
            client.showReplyDetails(requestID)
        else:
            print("No such client. See: 'help new' for more details")

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
            print("\nRunning command: '{}'...\n".format(command))
            self.parse(command)

        # then handle commands from the prompt
        while interactive:
            try:
                result = await self.cli.run_async()
                self.parse(result.text)
            except (EOFError, KeyboardInterrupt, Exit):
                return

        print('Goodbye.')

    def parse(self, cmdText):
        m = self.grammar.match(cmdText)
        if m:
            matchedVars = m.variables()
            self.logger.info("CLI command entered: {}".format(cmdText),
                             extra={"cli": False})

            # Check for helper commands
            if matchedVars.get('simple'):
                cmd = matchedVars.get('simple')
                if cmd == 'status':
                    self.getStatus()
                elif cmd == 'license':
                    self.printCmdHelper('license')
                elif cmd in ['exit', 'quit']:
                    raise Exit

            elif matchedVars.get('command') == 'help':
                helpable = matchedVars.get('helpable')
                if helpable:
                    self.printCmdHelper(command=helpable)
                else:
                    self.printHelp()

            elif matchedVars.get('command') == 'list':
                for cmd in self.commands:
                    print(cmd)

            # Check for new commands
            elif matchedVars.get('node_command') == 'new':
                name = matchedVars.get('node_name')
                self.newNode(name)

            elif matchedVars.get('node_command') == 'status':
                node = matchedVars.get('node_name')
                self.statusNode(node)

            elif matchedVars.get('node_command') == 'keyshare':
                name = matchedVars.get('node_name')
                self.keyshare(name)

            elif matchedVars.get('client_command') == 'new':
                client = matchedVars.get('client_name')
                self.newClient(client)

            elif matchedVars.get('client_command') == 'status':
                client = matchedVars.get('client_name')
                self.statusClient(client)

            elif matchedVars.get('client') == 'client':
                client_name = matchedVars.get('client_name')
                client_action = matchedVars.get('cli_action')
                if client_action == 'send':
                    msg = matchedVars.get('msg')
                    self.sendMsg(client_name, msg)
                elif client_action == 'show':
                    req_id = matchedVars.get('req_id')
                    self.getReply(client_name, req_id)
                else:
                    self.printCmdHelper("sendmsg")

            elif matchedVars.get('load') == 'load':
                file = matchedVars.get("file_name")
                if os.path.exists(file):
                    try:
                        self.loadFromFile(file)
                        print("Node registry loaded.")
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

            # Fall back to the help saying, invalid command.
            else:
                self.invalidCmd(cmdText)
        else:
            if cmdText != "":
                self.invalidCmd(cmdText)

    def invalidCmd(self, cmdText):
        print("Invalid command: '{}'\n".format(cmdText))
        self.printCmdHelper(command=None)

    def getNextAvailableAddr(self):
        self.curClientPort = self.curClientPort or 8100
        self.curClientPort += 1
        return "127.0.0.1", self.curClientPort


class Exit(Exception):
    pass
