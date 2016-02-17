from pygments.token import Token

import zeno.cli.cli as cli
from zeno.test.testable import Spyable
from zeno.test.helper import getAllArgs


@Spyable(methods=[cli.Cli.print, cli.Cli.printTokens])
class TestCli(cli.Cli):

    @property
    def lastPrintArgs(self):
        args = self.printeds
        if args:
            return args[0]
        return None

    @property
    def lastPrintTokenArgs(self):
        args = self.printedTokens
        if args:
            return args[0]
        return None

    @property
    def printeds(self):
        return getAllArgs(self, TestCli.print)

    @property
    def printedTokens(self):
        return getAllArgs(self, TestCli.printTokens)

    def enterCmd(self, cmd: str):
        self.parse(cmd)


def isErrorToken(token: Token):
    return token == Token.Error


def isHeadingToken(token: Token):
    return token == Token.Heading


def isNameToken(token: Token):
    return token == Token.Name
