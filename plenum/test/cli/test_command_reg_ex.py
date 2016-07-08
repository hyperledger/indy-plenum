from prompt_toolkit.contrib.regular_languages.compiler import compile

from plenum.cli.cli_helper import getUtilGrams, getNodeGrams, getClientGrams, getAllGrams


def test_command_reg_ex(cmd):
    utilGrams = getUtilGrams()
    nodeGrams = getNodeGrams()
    clientGrams = getClientGrams()
    grams = getAllGrams(utilGrams, nodeGrams, clientGrams)
    grammar = compile("".join(grams))
    res = grammar.match(cmd)
    assert res


def test_new_keypair_command_reg_ex():
    test_command_reg_ex("new keypair")