import re

import pytest
from plenum.common.constants import NYM, NODE
from plenum.common.roles import Roles
from plenum.common.transactions import PlenumTransactions
from plenum.test.cli.helper import _newStewardsAddedByName, \
    _newStewardsAddedByValue, _newNodesAddedByName, _newNodesAddedByValue


def testCreatePoolTxnFle(cli):
    _newStewardsAddedByName(cli)
    _newStewardsAddedByValue(cli),
    _newNodesAddedByName(cli),
    _newNodesAddedByValue(cli),
    assert len(cli.genesisTransactions) == 8
    cli.enterCmd("create genesis transaction file")
    assert not cli.lastCmdOutput.startswith("Invalid command")
    search = re.search("^Genesis transaction file created at (.*)$",
                       cli.lastCmdOutput,
                       re.MULTILINE)
    assert search
    filePath = search.group(1)
    assert filePath

