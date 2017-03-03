from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from plenum.test.cli.helper import checkReply, \
    checkSuccess, checkBalance, assertNoClient, loadPlugin, \
    createClientAndConnect
from plenum.test import waits

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


# @pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def testTransactions(cli, loadBankReqPlugin, createAllNodes, validNodeNames):
    numOfNodes = len(validNodeNames)
    timeout = waits.expectedTransactionExecutionTime(numOfNodes)

    def waitForEvent(event, nodeCount):
        cli.looper.run(eventually(checkReply, cli,
                                  nodeCount, event,
                                  timeout=timeout))

    def waitRequestSuccess(nodeCount):
        waitForEvent(checkSuccess, nodeCount)

    def waitBalanceChange(nodeCount, balanceValue):
        waitForEvent(partial(checkBalance, balanceValue), nodeCount)


    createClientAndConnect(cli, validNodeNames, "Alice")
    createClientAndConnect(cli, validNodeNames, "Bob")

    cli.enterCmd("client Alice credit 500 to Bob")
    waitRequestSuccess(numOfNodes)

    cli.enterCmd("client Alice balance")
    waitRequestSuccess(numOfNodes * 2)
    waitBalanceChange(numOfNodes, 500)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(numOfNodes * 3)
    waitBalanceChange(numOfNodes, 1500)

    cli.enterCmd("client Bob credit 10 to Alice")
    waitRequestSuccess(numOfNodes * 4)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(numOfNodes * 5)
    waitBalanceChange(numOfNodes, 1490)

    cli.enterCmd("client Bob credit 100 to Alice")
    waitRequestSuccess(numOfNodes * 6)

    cli.enterCmd("client Alice balance")
    waitRequestSuccess(numOfNodes * 7)
    waitBalanceChange(numOfNodes, 610)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(numOfNodes * 8)
    waitBalanceChange(numOfNodes, 1390)

    createClientAndConnect(cli, validNodeNames, "Carol")

    cli.enterCmd("client Carol credit 50 to Bob")
    waitRequestSuccess(numOfNodes * 9)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(numOfNodes * 10)
    waitBalanceChange(numOfNodes, 1440)

    cli.enterCmd("client Carol balance")
    waitRequestSuccess(numOfNodes * 11)
    waitBalanceChange(numOfNodes, 950)
