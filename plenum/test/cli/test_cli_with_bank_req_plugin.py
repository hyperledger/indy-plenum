import pytest

from plenum.test import waits
from plenum.test.cli.helper import \
    waitRequestSuccess, waitBalanceChange, \
    assertNoClient, loadPlugin, \
    createClientAndConnect


@pytest.fixture("module")
def loadBankReqPlugin(cli):
    loadPlugin(cli, 'bank_req_validation')
    loadPlugin(cli, 'bank_req_processor')


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def testReqForNonExistentClient(cli, loadBankReqPlugin, createAllNodes):
    cli.enterCmd("client Random balance")
    assertNoClient(cli)
    cli.enterCmd("client Random credit 400 to RandomNew")
    assertNoClient(cli)
    cli.enterCmd("client Random transactions")
    assertNoClient(cli)


def testTransactions(cli, loadBankReqPlugin, createAllNodes, validNodeNames):
    nodeCount = len(validNodeNames)

    createClientAndConnect(cli, validNodeNames, "Alice")
    createClientAndConnect(cli, validNodeNames, "Bob")

    timeout = waits.expectedTransactionExecutionTime(nodeCount)

    cli.enterCmd("client Alice credit 500 to Bob")
    waitRequestSuccess(cli, nodeCount, customTimeout=timeout)

    cli.enterCmd("client Alice balance")
    waitRequestSuccess(cli, nodeCount * 2, customTimeout=timeout)
    waitBalanceChange(cli, nodeCount, 500, customTimeout=timeout)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(cli, nodeCount * 3, customTimeout=timeout)
    waitBalanceChange(cli, nodeCount, 1500, customTimeout=timeout)

    cli.enterCmd("client Bob credit 10 to Alice")
    waitRequestSuccess(cli, nodeCount * 4, customTimeout=timeout)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(cli, nodeCount * 5, customTimeout=timeout)
    waitBalanceChange(cli, nodeCount, 1490, customTimeout=timeout)

    cli.enterCmd("client Bob credit 100 to Alice")
    waitRequestSuccess(cli, nodeCount * 6, customTimeout=timeout)

    cli.enterCmd("client Alice balance")
    waitRequestSuccess(cli, nodeCount * 7, customTimeout=timeout)
    waitBalanceChange(cli, nodeCount, 610, customTimeout=timeout)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(cli, nodeCount * 8, customTimeout=timeout)
    waitBalanceChange(cli, nodeCount, 1390, customTimeout=timeout)

    createClientAndConnect(cli, validNodeNames, "Carol")

    cli.enterCmd("client Carol credit 50 to Bob")
    waitRequestSuccess(cli, nodeCount * 9, customTimeout=timeout)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(cli, nodeCount * 10, customTimeout=timeout)
    waitBalanceChange(cli, nodeCount, 1440, customTimeout=timeout)

    cli.enterCmd("client Carol balance")
    waitRequestSuccess(cli, nodeCount * 11, customTimeout=timeout)
    waitBalanceChange(cli, nodeCount, 950, customTimeout=timeout)
