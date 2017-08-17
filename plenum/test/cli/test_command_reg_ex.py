import pytest
from plenum.cli.helper import getUtilGrams, getNodeGrams, getClientGrams, \
    getAllGrams
from plenum.common.constants import TXN_TYPE, TARGET_NYM, DATA, IDENTIFIER, NODE, \
    NYM, ROLE
from plenum.common.roles import Roles
from plenum.common.transactions import PlenumTransactions
from plenum.test.cli.helper import assertCliTokens
from prompt_toolkit.contrib.regular_languages.compiler import compile


@pytest.fixture("module")
def grammar():
    utilGrams = getUtilGrams()
    nodeGrams = getNodeGrams()
    clientGrams = getClientGrams()
    grams = getAllGrams(utilGrams, nodeGrams, clientGrams)
    return compile("".join(grams))


def getMatchedVariables(grammar, cmd):
    m = grammar.match(cmd)
    assert m, "Given command didn't match with any cli reg ex"
    return m.variables()


def testListRegEx(grammar):
    matchedVars = getMatchedVariables(grammar, "list")
    assertCliTokens(matchedVars, {"command": "list", "sorted": None})

    matchedVars = getMatchedVariables(grammar, "list sorted")
    assertCliTokens(matchedVars, {"command": "list", "sorted": "sorted"})


def testUseKeyringRegEx(grammar):
    matchedVars = getMatchedVariables(grammar, "use wallet abc")
    assertCliTokens(matchedVars, {"use_wallet": "use wallet",
                                  "wallet": "abc", "copy_as": None,
                                  "copy-as-name": None, "override": None})
    matchedVars = getMatchedVariables(grammar, "use wallet abc ")
    assertCliTokens(matchedVars, {"use_wallet": "use wallet", "wallet": "abc",
                                  "copy_as": None, "copy_as_name": None,
                                  "override": None})

    matchedVars = getMatchedVariables(
        grammar, "use wallet abc copy as newwallet")
    assertCliTokens(matchedVars,
                    {"use_wallet": "use wallet",
                     "wallet": "abc",
                     "copy_as": "copy as",
                     "copy_as_name": "newwallet",
                     "override": None})

    matchedVars = getMatchedVariables(
        grammar, "use wallet abc copy as newwallet " "override")
    assertCliTokens(matchedVars,
                    {"use_wallet": "use wallet",
                     "wallet": "abc",
                     "copy_as": "copy as",
                     "copy_as_name": "newwallet",
                     "override": "override"})

    matchedVars = getMatchedVariables(grammar, "use wallet abc override")
    assertCliTokens(matchedVars, {"use_wallet": "use wallet", "wallet": "abc",
                                  "copy_as": None, "copy_as_name": None,
                                  "override": "override"})


def testSaveKeyringRegEx(grammar):
    matchedVars = getMatchedVariables(grammar, "save wallet")
    assertCliTokens(
        matchedVars, {"save_wallet": "save wallet", "wallet": None})
    matchedVars = getMatchedVariables(grammar, "save wallet default")
    assertCliTokens(matchedVars, {"save_wallet": "save wallet",
                                  "wallet": "default"})


def testPromptCommandRegEx(grammar):
    matchedVars = getMatchedVariables(grammar, "prompt Alice")
    assertCliTokens(matchedVars, {"prompt": "prompt", "name": "Alice"})
    matchedVars = getMatchedVariables(grammar, "prompt Alice ")
    assertCliTokens(matchedVars, {"prompt": "prompt", "name": "Alice"})


def testListKeyringsCommandRegEx(grammar):
    matchedVars = getMatchedVariables(grammar, "list wallets")
    assertCliTokens(matchedVars, {"list_wallets": "list wallets"})


def testNewKeyRingCommandRegEx(grammar):
    matchedVars = getMatchedVariables(grammar, "new wallet MyKey1")
    assertCliTokens(
        matchedVars, {"new_wallet": "new wallet", "name": "MyKey1"})
    matchedVars = getMatchedVariables(grammar, "new wallet MyKey1 ")
    assertCliTokens(
        matchedVars, {"new_wallet": "new wallet", "name": "MyKey1"})


def testRenameKeyRingCommandRegEx(grammar):
    matchedVars = getMatchedVariables(
        grammar, "rename wallet MyKey1 to MyKey2")
    assertCliTokens(
        matchedVars, {
            "rename_wallet": "rename wallet", "from": "MyKey1", "to": "MyKey2"})
    matchedVars = getMatchedVariables(grammar, "rename wallet to MyKey2")
    assertCliTokens(
        matchedVars, {
            "rename_wallet": "rename wallet", "from": None, "to": "MyKey2"})


def testNewKeypairCommandRegEx(grammar):
    matchedVars = getMatchedVariables(grammar, "new key")
    assertCliTokens(
        matchedVars, {"new_key": "new key", "alias": None, "seed": None})

    matchedVars = getMatchedVariables(grammar, "new key test")
    assertCliTokens(
        matchedVars, {"new_key": "new key", "alias": "test", "seed": None})

    matchedVars = getMatchedVariables(grammar, "new key as test")
    assertCliTokens(
        matchedVars, {"new_key": "new key", "alias": "test", "seed": None})

    matchedVars = getMatchedVariables(
        grammar, "new key with seed " + 's' * 32 + " as test")
    assertCliTokens(
        matchedVars, {"new_key": "new key", "alias": "test", "seed": 's' * 32})

    matchedVars = getMatchedVariables(
        grammar, "new key with seed " + 's' * 32 + " test")
    assertCliTokens(
        matchedVars, {"new_key": "new key", "alias": "test", "seed": 's' * 32})

    matchedVars = getMatchedVariables(grammar, "new key with seed " + 's' * 32)
    assertCliTokens(
        matchedVars, {"new_key": "new key", "alias": None, "seed": 's' * 32})


def testNewListIdsRegEx(grammar):
    getMatchedVariables(grammar, "list ids")


def testAddGenTxnByNameRegEx(grammar):
    matchedVars = getMatchedVariables(
        grammar, "add genesis transaction {nym} for Tyler role={role}".format(
            nym=PlenumTransactions.NYM.name, role=Roles.STEWARD.name))
    assertCliTokens(matchedVars,
                    {TXN_TYPE: PlenumTransactions.NYM.name,
                     ROLE: Roles.STEWARD.name,
                     TARGET_NYM: "Tyler",
                     DATA: None})

    matchedVars = getMatchedVariables(
        grammar,
        'add genesis transaction {nym} for Tyler with data {{"key1": "value1"}} role={role}'.format(
            nym=PlenumTransactions.NYM.name,
            role=Roles.STEWARD.name))
    assertCliTokens(matchedVars,
                    {TXN_TYPE: PlenumTransactions.NYM.name,
                     ROLE: Roles.STEWARD.name,
                     TARGET_NYM: "Tyler",
                     DATA: '{"key1": "value1"}'})

    matchedVars = getMatchedVariables(
        grammar,
        'add genesis transaction {node} for Tyler by Phil with data {{"key1": "value1", "key2": "value2"}}'.format(
            node=PlenumTransactions.NODE.name))
    assertCliTokens(matchedVars,
                    {TXN_TYPE: PlenumTransactions.NODE.name,
                     TARGET_NYM: "Tyler",
                     IDENTIFIER: "Phil",
                     DATA: '{"key1": "value1", "key2": "value2"}'})


def testAddGenTxnByValueRegEx(grammar):
    matchedVars = getMatchedVariables(
        grammar, "add genesis transaction {nym} for Tyler role={role}".format(
            nym=NYM, role=Roles.STEWARD.name))
    assertCliTokens(matchedVars,
                    {TXN_TYPE: NYM,
                     ROLE: Roles.STEWARD.name,
                     TARGET_NYM: "Tyler",
                     DATA: None})

    matchedVars = getMatchedVariables(
        grammar,
        'add genesis transaction {nym} for Tyler with data {{"key1": "value1"}} role={role}'.format(
            nym=NYM,
            role=Roles.STEWARD.name))
    assertCliTokens(matchedVars,
                    {TXN_TYPE: NYM,
                     ROLE: Roles.STEWARD.name,
                     TARGET_NYM: "Tyler",
                     DATA: '{"key1": "value1"}'})

    matchedVars = getMatchedVariables(
        grammar,
        'add genesis transaction {node} for Tyler by Phil with data {{"key1": "value1", "key2": "value2"}}'.format(
            node=NODE))
    assertCliTokens(matchedVars,
                    {TXN_TYPE: NODE,
                     TARGET_NYM: "Tyler",
                     IDENTIFIER: "Phil",
                     DATA: '{"key1": "value1", "key2": "value2"}'})


def testNewAddGenTxnByValueRegEx(grammar):
    exportedData = """{"BCU-steward": {"verkey": "b0739fe3113adbdce9dd994057bed5339e9bf2f99a6b7d4754b8b9d094e7c1e0"}}"""
    matchedVars = getMatchedVariables(
        grammar, 'add genesis transaction {nym} with data {data} role={role}'.format(
            nym=NYM, data=exportedData, role=Roles.STEWARD.name))
    assertCliTokens(
        matchedVars, {
            TXN_TYPE: NYM, ROLE: Roles.STEWARD.name, DATA: exportedData})

    exportedData = """{"BCU": {"verkey": "ad1a8dc1836007587f6c6c2d1d6ba91a395616f923b3e63bb5797d52b025a263",
    "node_address": "127.0.0.1:9701",
    "client_address": "127.0.0.1:9702"}, "by":ea0690fbea7fbcd8dd4b80ed83f23d0ff2152e6217f602a01532c16c862aab92}"""
    matchedVars = getMatchedVariables(
        grammar, 'add genesis transaction {node} with data {data}'.format(
            node=NODE, data=exportedData))
    assertCliTokens(matchedVars, {TXN_TYPE: NODE, DATA: exportedData})


def testNewAddGenTxnByNameRegEx(grammar):
    exportedData = """{"BCU-steward": {"verkey": "b0739fe3113adbdce9dd994057bed5339e9bf2f99a6b7d4754b8b9d094e7c1e0"}}"""
    matchedVars = getMatchedVariables(
        grammar,
        'add genesis transaction {nym} with data {data} role={role}'.format(
            nym=PlenumTransactions.NYM.name,
            data=exportedData,
            role=Roles.STEWARD.name))
    assertCliTokens(matchedVars,
                    {TXN_TYPE: PlenumTransactions.NYM.name,
                     ROLE: Roles.STEWARD.name,
                     DATA: exportedData})

    exportedData = """{"BCU": {"verkey": "ad1a8dc1836007587f6c6c2d1d6ba91a395616f923b3e63bb5797d52b025a263",
    "node_address": "127.0.0.1:9701",
    "client_address": "127.0.0.1:9702"}, "by":ea0690fbea7fbcd8dd4b80ed83f23d0ff2152e6217f602a01532c16c862aab92}"""
    matchedVars = getMatchedVariables(
        grammar, 'add genesis transaction {node} with data {data}'.format(
            node=PlenumTransactions.NODE.name, data=exportedData))
    assertCliTokens(
        matchedVars, {
            TXN_TYPE: PlenumTransactions.NODE.name, DATA: exportedData})


def testCreateGenesisTxnFileRegEx(grammar):
    matchedVars = getMatchedVariables(
        grammar, "create genesis transaction file")
    assert matchedVars
