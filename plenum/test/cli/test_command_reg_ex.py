import pytest
from prompt_toolkit.contrib.regular_languages.compiler import compile
from plenum.cli.helper import getUtilGrams, getNodeGrams, getClientGrams, getAllGrams


@pytest.fixture("module")
def grammar():
    utilGrams = getUtilGrams()
    nodeGrams = getNodeGrams()
    clientGrams = getClientGrams()
    grams = getAllGrams(utilGrams, nodeGrams, clientGrams)
    return compile("".join(grams))


@pytest.fixture("module")
def checkIfMatched(grammar, cmd):
    assert grammar.match(cmd)


def test_new_keypair_command_reg_ex(grammar):
    checkIfMatched(grammar, "new keypair")

def test_new_list_ids_reg_ex(grammar):
    checkIfMatched(grammar, "list ids")
