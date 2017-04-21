import pytest

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


# @pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def testTransactions(cli, loadBankReqPlugin, createAllNodes, validNodeNames):
    numOfNodes = len(validNodeNames)

    createClientAndConnect(cli, validNodeNames, "Alice")
    createClientAndConnect(cli, validNodeNames, "Bob")

    cli.enterCmd("client Alice credit 500 to Bob")
    waitRequestSuccess(cli, numOfNodes)

    cli.enterCmd("client Alice balance")
    waitRequestSuccess(cli, numOfNodes * 2)
    waitBalanceChange(cli, numOfNodes, 500)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(cli, numOfNodes * 3)
    waitBalanceChange(cli, numOfNodes, 1500)

    cli.enterCmd("client Bob credit 10 to Alice")
    waitRequestSuccess(cli, numOfNodes * 4)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(cli, numOfNodes * 5)
    waitBalanceChange(cli, numOfNodes, 1490)

    cli.enterCmd("client Bob credit 100 to Alice")
    waitRequestSuccess(cli, numOfNodes * 6)

    cli.enterCmd("client Alice balance")
    waitRequestSuccess(cli, numOfNodes * 7)
    waitBalanceChange(cli, numOfNodes, 610)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(cli, numOfNodes * 8)
    waitBalanceChange(cli, numOfNodes, 1390)

    createClientAndConnect(cli, validNodeNames, "Carol")

    cli.enterCmd("client Carol credit 50 to Bob")
    waitRequestSuccess(cli, numOfNodes * 9)

    cli.enterCmd("client Bob balance")
    waitRequestSuccess(cli, numOfNodes * 10)
    waitBalanceChange(cli, numOfNodes, 1440)

    cli.enterCmd("client Carol balance")
    waitRequestSuccess(cli, numOfNodes * 11)
    waitBalanceChange(cli, numOfNodes, 950)
