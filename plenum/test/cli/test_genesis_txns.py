import pytest
import re


@pytest.fixture("module")
def newStewardsAdded(cli):
    assert len(cli.genesisTransactions) == 0
    cli.enterCmd("add genesis transaction NEW_STEWARD for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418")
    assert len(cli.genesisTransactions) == 1
    assert cli.lastCmdOutput == "Genesis transaction added"
    cli.enterCmd(
        'add genesis transaction NEW_STEWARD for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06419 with data {"alias": "Ty", "pubkey": "59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06420"}')
    assert len(cli.genesisTransactions) == 2
    assert cli.lastCmdOutput == "Genesis transaction added"
    return cli


@pytest.fixture("module")
def newNodesAdded(cli):
    assert len(cli.genesisTransactions) == 2
    cli.enterCmd('add genesis transaction NEW_NODE for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 by 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 with data '
                 '{"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", '
                 '"client_port": "9702", "pubkey": "59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06422", '
                 '"alias": "PhilNode"}')
    assert len(cli.genesisTransactions) == 3
    assert cli.lastCmdOutput == "Genesis transaction added"
    cli.enterCmd('add genesis transaction NEW_NODE for 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06419 by 59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06418 with data '
                 '{"node_ip": "localhost", "node_port": "9701", "client_ip": "localhost", '
                 '"client_port": "9702", "pubkey": "59d9225473451efffe6b36dbcaefdbf7b1895de62084509a7f5b58bf01d06423", '
                 '"alias": "PhilNode"}')
    assert len(cli.genesisTransactions) == 4
    assert cli.lastCmdOutput == "Genesis transaction added"
    return cli


def test_new_steward_gen_txn(newStewardsAdded):
    pass


def test_new_node_gen_txn(newNodesAdded):
    pass


def test_create_pool_txn_fle(newStewardsAdded, newNodesAdded):
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
