import re

import pytest
from plenum.common.constants import NYM, NODE
from plenum.common.roles import Roles
from plenum.common.transactions import PlenumTransactions


@pytest.fixture("module")
def newStewardsAddedByName(cli):
    oldGenTxns = len(cli.genesisTransactions)
    cli.enterCmd(
        "add genesis transaction {nym} for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 role={role}".format(
            nym=PlenumTransactions.NYM.name, role=Roles.STEWARD.name))
    assert len(cli.genesisTransactions) == oldGenTxns + 1
    assert cli.lastCmdOutput == "Genesis transaction added"
    cli.enterCmd(
        'add genesis transaction {nym} for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06419 '
        'with data {{"alias": "Ty"}} role={role}'.format(nym=PlenumTransactions.NYM.name, role=Roles.STEWARD.name))
    assert len(cli.genesisTransactions) == oldGenTxns + 2
    assert cli.lastCmdOutput == "Genesis transaction added"
    return cli


@pytest.fixture("module")
def newStewardsAddedByValue(cli):
    oldGenTxns = len(cli.genesisTransactions)
    cli.enterCmd(
        "add genesis transaction {nym} for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06420 role={role}".format(
            nym=NYM, role=Roles.STEWARD.name))
    assert len(cli.genesisTransactions) == oldGenTxns + 1
    assert cli.lastCmdOutput == "Genesis transaction added"
    cli.enterCmd(
        'add genesis transaction {nym} for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06421 '
        'with data {{"alias": "Ty"}} role={role}'.format(nym=NYM, role=Roles.STEWARD.name))
    assert len(cli.genesisTransactions) == oldGenTxns + 2
    assert cli.lastCmdOutput == "Genesis transaction added"
    return cli


@pytest.fixture("module")
def newNodesAddedByName(cli):
    oldGenTxns = len(cli.genesisTransactions)
    cli.enterCmd(
        'add genesis transaction {node} for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 by 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 with data '
        '{{"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", '
        '"client_port": "9702", '
        '"alias": "PhilNode"}}'.format(node=PlenumTransactions.NODE.name))
    assert len(cli.genesisTransactions) == oldGenTxns + 1
    assert cli.lastCmdOutput == "Genesis transaction added"
    cli.enterCmd(
        'add genesis transaction {node} for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06419 by 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 with data '
        '{{"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", '
        '"client_port": "9702", '
        '"alias": "PhilNode"}}'.format(node=PlenumTransactions.NODE.name))
    assert len(cli.genesisTransactions) == oldGenTxns + 2
    assert cli.lastCmdOutput == "Genesis transaction added"
    return cli


@pytest.fixture("module")
def newNodesAddedByValue(cli):
    oldGenTxns = len(cli.genesisTransactions)
    cli.enterCmd(
        'add genesis transaction {node} for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06420 by 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06420 with data '
        '{{"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", '
        '"client_port": "9702", '
        '"alias": "PhilNode"}}'.format(node=NODE))
    assert len(cli.genesisTransactions) == oldGenTxns + 1
    assert cli.lastCmdOutput == "Genesis transaction added"
    cli.enterCmd(
        'add genesis transaction {node} for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06421 by 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06420 with data '
        '{{"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", '
        '"client_port": "9702", '
        '"alias": "PhilNode"}}'.format(node=NODE))
    assert len(cli.genesisTransactions) == oldGenTxns + 2
    assert cli.lastCmdOutput == "Genesis transaction added"
    return cli


def testNewStewardGenTxnByName(newStewardsAddedByName):
    pass


def testNewStewardGenTxnByValue(newStewardsAddedByValue):
    pass


def testNewNodeGenTxnByName(newNodesAddedByName):
    pass


def testNewNodeGenTxnByValue(newNodesAddedByValue):
    pass


def testCreatePoolTxnFle(newStewardsAddedByName, newStewardsAddedByValue, newNodesAddedByName, newNodesAddedByValue):
    cli = newNodesAddedByValue
    assert len(cli.genesisTransactions) == 8
    cli.enterCmd("create genesis transaction file")
    assert not cli.lastCmdOutput.startswith("Invalid command")
    search = re.search("^Genesis transaction file created at (.*)$",
                       cli.lastCmdOutput,
                       re.MULTILINE)
    assert search
    filePath = search.group(1)
    assert filePath


@pytest.fixture("module")
def newAddGenStewardTxnAdded(cli):
    oldGenTxns = len(cli.genesisTransactions)
    exportedData = """{"BCU-steward": {"verkey": "b0739fe3113adbdce9dd994057bed5339e9bf2f99a6b7d4754b8b9d094e7c1e0"}}"""
    cli.enterCmd(
        'add genesis transaction {nym} with data {data} role={role}'.format(nym=PlenumTransactions.NYM.name,
                                                                            data=exportedData, role=Roles.STEWARD.name))
    assert len(cli.genesisTransactions) == oldGenTxns + 1


def testAddNewStewardAddGenTxn(newAddGenStewardTxnAdded):
    pass


@pytest.fixture("module")
def newAddGenNodeTxnAdded(cli):
    oldGenTxns = len(cli.genesisTransactions)
    exportedData = """{"BCU": {"verkey": "3932de7cd1434d96e20780ba7f3034529f684d65c4f8ffdb790a1c921db79382",
    "node_address": "127.0.0.1:9701","client_address": "127.0.0.1:9702"},
    "by":"b0739fe3113adbdce9dd994057bed5339e9bf2f99a6b7d4754b8b9d094e7c1e0"}"""
    cli.enterCmd(
        'add genesis transaction {node} with data {data}'.format(node=PlenumTransactions.NODE.name, data=exportedData))
    assert len(cli.genesisTransactions) == oldGenTxns + 1


def testAddNewNodeAddGenTxn(newAddGenNodeTxnAdded):
    pass
