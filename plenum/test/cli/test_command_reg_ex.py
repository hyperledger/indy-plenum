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
def getMatchedVariables(grammar, cmd):
    m = grammar.match(cmd)
    assert m
    return m.variables()


def test_new_keypair_command_reg_ex(grammar):
    getMatchedVariables(grammar, "new key test")


def test_new_list_ids_reg_ex(grammar):
    getMatchedVariables(grammar, "list ids")


def test_add_gen_txn_reg_ex(grammar):
    matchedVars = getMatchedVariables(grammar, "add genesis transaction NEW_STEWARD for Tyler")
    assert matchedVars.get("txn_type") == "NEW_STEWARD"
    assert matchedVars.get("dest") == "Tyler"
    assert matchedVars.get("data") is None

    matchedVars = getMatchedVariables(grammar, 'add genesis transaction NEW_STEWARD for Tyler with data {"key1": "value1"}')
    assert matchedVars.get("txn_type") == "NEW_STEWARD"
    assert matchedVars.get("dest") == "Tyler"
    assert matchedVars.get("data") == '{"key1": "value1"}'

    matchedVars = getMatchedVariables(grammar,
                                      'add genesis transaction NEW_NODE for Tyler by Phil with data {"key1": "value1", "key2": "value2"}')
    assert matchedVars.get("txn_type") == "NEW_NODE"
    assert matchedVars.get("dest") == "Tyler"
    assert matchedVars.get("identifier") == "Phil"
    assert matchedVars.get("data") == '{"key1": "value1", "key2": "value2"}'


def test_create_genesis_txn_file_reg_ex(grammar):
    matchedVars = getMatchedVariables(grammar, "create genesis transaction file")
    assert matchedVars

