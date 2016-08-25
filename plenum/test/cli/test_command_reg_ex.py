import pytest
from prompt_toolkit.contrib.regular_languages.compiler import compile
from plenum.cli.helper import getUtilGrams, getNodeGrams, getClientGrams, getAllGrams
from plenum.common.txn import TXN_TYPE, TARGET_NYM, DATA, IDENTIFIER, NEW_NODE, \
    NYM, ROLE, STEWARD


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


def assertCliTokens(matchedVars, tokens):
    for key, value in tokens.items():
        assert matchedVars.get(key) == value


def testNewKeypairCommandRegEx(grammar):
    matchedVars = getMatchedVariables(grammar, "new key")
    assertCliTokens(matchedVars, {"new_key":"new key", "alias":None, "seed":None})

    matchedVars = getMatchedVariables(grammar, "new key test")
    assertCliTokens(matchedVars, {"new_key": "new key", "alias": "test", "seed": None})

    matchedVars = getMatchedVariables(grammar, "new key as test")
    assertCliTokens(matchedVars, {"new_key": "new key", "alias": "test", "seed": None})

    matchedVars = getMatchedVariables(grammar, "new key with seed " + 's'*32 + " as test")
    assertCliTokens(matchedVars, {"new_key": "new key", "alias": "test", "seed": 's'*32})

    matchedVars = getMatchedVariables(grammar, "new key with seed " + 's' * 32 + " test")
    assertCliTokens(matchedVars, {"new_key": "new key", "alias": "test", "seed": 's' * 32})

    matchedVars = getMatchedVariables(grammar, "new key with seed " + 's' * 32)
    assertCliTokens(matchedVars, {"new_key": "new key", "alias": None, "seed": 's' * 32})


def testNewListIdsRegEx(grammar):
    getMatchedVariables(grammar, "list ids")


def testAddGenTxnRegEx(grammar):
    matchedVars = getMatchedVariables(grammar, "add genesis transaction NYM for Tyler role=STEWARD")
    assertCliTokens(matchedVars, {TXN_TYPE: NYM, ROLE: STEWARD, TARGET_NYM: "Tyler", DATA: None})

    matchedVars = getMatchedVariables(grammar, 'add genesis transaction NYM for Tyler with data {"key1": "value1"} role=STEWARD')
    assertCliTokens(matchedVars, {TXN_TYPE: NYM, ROLE: STEWARD, TARGET_NYM: "Tyler", DATA: '{"key1": "value1"}'})

    matchedVars = getMatchedVariables(grammar,
                                      'add genesis transaction NEW_NODE for Tyler by Phil with data {"key1": "value1", "key2": "value2"}')
    assertCliTokens(matchedVars, {TXN_TYPE: NEW_NODE, TARGET_NYM: "Tyler", IDENTIFIER: "Phil",
                                  DATA: '{"key1": "value1", "key2": "value2"}'})


def testNewAddGenTxnRegEx(grammar):
    exportedData = """{"BCU-steward": {"verkey": "b0739fe3113adbdce9dd994057bed5339e9bf2f99a6b7d4754b8b9d094e7c1e0"}}"""
    matchedVars = getMatchedVariables(grammar,
                                      'add genesis transaction NYM with data {} role=STEWARD'.format(exportedData))
    assertCliTokens(matchedVars, {TXN_TYPE: NYM, ROLE: STEWARD, DATA: exportedData})

    exportedData = """{"BCU": {"verkey": "ad1a8dc1836007587f6c6c2d1d6ba91a395616f923b3e63bb5797d52b025a263",
    "node_address": "127.0.0.1:9701",
    "client_address": "127.0.0.1:9702"}, "by":ea0690fbea7fbcd8dd4b80ed83f23d0ff2152e6217f602a01532c16c862aab92}"""
    matchedVars = getMatchedVariables(grammar,
                                      'add genesis transaction NEW_NODE with data {}'.format(exportedData))
    assertCliTokens(matchedVars, {TXN_TYPE: NEW_NODE, DATA: exportedData})


def testCreateGenesisTxnFileRegEx(grammar):
    matchedVars = getMatchedVariables(grammar, "create genesis transaction file")
    assert matchedVars

