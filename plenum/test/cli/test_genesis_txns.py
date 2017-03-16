import re

import pytest
from plenum.common.roles import Roles


@pytest.fixture("module")
def newStewardsAdded(cli):
    oldGenTxns = len(cli.genesisTransactions)
    cli.enterCmd(
        "add genesis transaction NYM for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 role={role}".format(
            role=Roles.STEWARD.name))
    assert len(cli.genesisTransactions) == oldGenTxns + 1
    assert cli.lastCmdOutput == "Genesis transaction added"
    cli.enterCmd(
        'add genesis transaction NYM for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06419 '
        'with data {{"alias": "Ty"}} role={role}'.format(role=Roles.STEWARD.name))
    assert len(cli.genesisTransactions) == oldGenTxns + 2
    assert cli.lastCmdOutput == "Genesis transaction added"
    return cli


@pytest.fixture("module")
def newNodesAdded(cli):
    oldGenTxns = len(cli.genesisTransactions)
    cli.enterCmd(
        'add genesis transaction NODE for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 by 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 with data '
        '{"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", '
        '"client_port": "9702", '
        '"alias": "PhilNode"}')
    assert len(cli.genesisTransactions) == oldGenTxns + 1
    assert cli.lastCmdOutput == "Genesis transaction added"
    cli.enterCmd(
        'add genesis transaction NODE for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06419 by 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 with data '
        '{"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", '
        '"client_port": "9702", '
        '"alias": "PhilNode"}')
    assert len(cli.genesisTransactions) == oldGenTxns + 2
    assert cli.lastCmdOutput == "Genesis transaction added"
    return cli


def testNewStewardGenTxn(newStewardsAdded):
    pass


def testNewNodeGenTxn(newNodesAdded):
    pass


def testCreatePoolTxnFle(newStewardsAdded, newNodesAdded):
    cli = newNodesAdded
    assert len(cli.genesisTransactions) == 4
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
        'add genesis transaction NYM with data {data} role={role}'.format(data=exportedData, role=Roles.STEWARD.name))
    assert len(cli.genesisTransactions) == oldGenTxns + 1


def testAddNewStewardAddGenTxn(newAddGenStewardTxnAdded):
    pass


@pytest.fixture("module")
def newAddGenNodeTxnAdded(cli):
    oldGenTxns = len(cli.genesisTransactions)
    exportedData = """{"BCU": {"verkey": "3932de7cd1434d96e20780ba7f3034529f684d65c4f8ffdb790a1c921db79382",
    "node_address": "127.0.0.1:9701","client_address": "127.0.0.1:9702"},
    "by":"b0739fe3113adbdce9dd994057bed5339e9bf2f99a6b7d4754b8b9d094e7c1e0"}"""
    cli.enterCmd('add genesis transaction NODE with data {}'.format(exportedData))
    assert len(cli.genesisTransactions) == oldGenTxns + 1


def testAddNewNodeAddGenTxn(newAddGenNodeTxnAdded):
    pass
