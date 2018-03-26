import re

import pytest
from plenum.common.constants import NYM, NODE
from plenum.common.roles import Roles
from plenum.common.transactions import PlenumTransactions
from plenum.test.cli.helper import _newStewardsAddedByName, \
    _newStewardsAddedByValue, _newNodesAddedByName, _newNodesAddedByValue


@pytest.fixture("function")
def oldGenTxns(cli):
    return len(cli.genesisTransactions)

def testNewStewardGenTxnByName(oldGenTxns, cli):
    _newStewardsAddedByName(cli)
    assert len(cli.genesisTransactions) == oldGenTxns + 2
    assert cli.lastCmdOutput == "Genesis transaction added"


def testNewStewardGenTxnByValue(oldGenTxns, cli):
    _newStewardsAddedByValue(cli)
    assert len(cli.genesisTransactions) == oldGenTxns + 2
    assert cli.lastCmdOutput == "Genesis transaction added"


def testNewNodeGenTxnByName(oldGenTxns, cli):
    _newNodesAddedByName(cli)
    assert len(cli.genesisTransactions) == oldGenTxns + 2
    assert cli.lastCmdOutput == "Genesis transaction added"


def testNewNodeGenTxnByValue(oldGenTxns, cli):
    _newNodesAddedByValue(cli)
    assert len(cli.genesisTransactions) == oldGenTxns + 2
    assert cli.lastCmdOutput == "Genesis transaction added"


def _newAddGenStewardTxnAdded(cli):
    exportedData = """{"BCU-steward": {"verkey": "b0739fe3113adbdce9dd994057bed5339e9bf2f99a6b7d4754b8b9d094e7c1e0"}}"""
    cli.enterCmd('add genesis transaction {nym} with data {data} role={role}'.format(
        nym=PlenumTransactions.NYM.name, data=exportedData, role=Roles.STEWARD.name))


def testAddNewStewardAddGenTxn(oldGenTxns, cli):
    _newAddGenStewardTxnAdded(cli)
    assert len(cli.genesisTransactions) == oldGenTxns + 1


def _newAddGenNodeTxnAdded(cli):
    exportedData = """{"BCU": {"verkey": "3932de7cd1434d96e20780ba7f3034529f684d65c4f8ffdb790a1c921db79382",
    "node_address": "127.0.0.1:9701","client_address": "127.0.0.1:9702"},
    "by":"b0739fe3113adbdce9dd994057bed5339e9bf2f99a6b7d4754b8b9d094e7c1e0"}"""
    cli.enterCmd('add genesis transaction {node} with data {data}'.format(
        node=PlenumTransactions.NODE.name, data=exportedData))


def testAddNewNodeAddGenTxn(oldGenTxns, cli):
    _newAddGenNodeTxnAdded(cli)
    assert len(cli.genesisTransactions) == oldGenTxns + 1
