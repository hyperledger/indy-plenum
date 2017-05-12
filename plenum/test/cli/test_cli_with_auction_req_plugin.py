from functools import partial
from uuid import uuid4

import pytest

from stp_core.loop.eventually import eventually
from plenum.test.cli.helper import loadPlugin, assertNoClient, \
    createClientAndConnect, waitRequestSuccess, waitBalanceChange


@pytest.fixture("module")
def loadAuctionReqPlugin(cli):
    loadPlugin(cli, 'auction_req_validation')
    loadPlugin(cli, 'auction_req_processor')


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def testReqForNonExistentClient(cli, loadAuctionReqPlugin, createAllNodes):
    cli.enterCmd("client Random balance")
    assertNoClient(cli)
    aid = "09d7f84f-a42f-449c-9477-cf7c3fbe778c"
    cli.enterCmd("client Random start auction {}".format(aid))
    assertNoClient(cli)
    cli.enterCmd("client Random end auction {}".format(aid))
    assertNoClient(cli)
    cli.enterCmd("client Random place bid 40 on {}".format(aid))
    assertNoClient(cli)

# TODO: Have a test for non existent auction id


# @pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
@pytest.mark.skip(reason='SOV-1020')
def testTransactions(cli, loadAuctionReqPlugin, createAllNodes, validNodeNames):
    nodeCount = len(validNodeNames)
    auctionId = str(uuid4())
    names = ["Jason", "John", "Les", "Timothy", "Tyler"]
    for name in names:
        createClientAndConnect(cli, validNodeNames, name)
    jason, tyler, les, john, timothy = names

    cli.enterCmd("client {} start auction {}".format(jason, auctionId))
    waitRequestSuccess(cli, nodeCount * 1)

    cli.enterCmd("client {} place bid 20 on {}".format(tyler, auctionId))
    waitRequestSuccess(cli, nodeCount * 2)

    cli.enterCmd("client {} balance".format(tyler))
    waitRequestSuccess(cli, nodeCount * 3)

    waitBalanceChange(cli, nodeCount, 980)

    cli.enterCmd("client {} place bid 40 on {}".format(les, auctionId))
    waitRequestSuccess(cli, nodeCount * 4)

    cli.enterCmd("client {} balance".format(tyler))
    waitRequestSuccess(cli, nodeCount * 5)
    waitBalanceChange(cli, nodeCount, 1000)

    cli.enterCmd("client {} balance".format(les))
    waitRequestSuccess(cli, nodeCount * 6)
    waitBalanceChange(cli, nodeCount, 960)

    # This bid would fail so `success` would be false and thus success count
    # wont increase
    cli.enterCmd("client {} place bid 30 on {}".format(john, auctionId))
    waitRequestSuccess(cli, nodeCount * 6)

    cli.enterCmd("client {} balance".format(john))
    waitRequestSuccess(cli, nodeCount * 7)
    waitBalanceChange(cli, nodeCount * 2, 1000)

    cli.enterCmd("client {} balance".format(les))
    waitRequestSuccess(cli, nodeCount * 8)
    waitBalanceChange(cli, nodeCount * 2, 960)

    cli.enterCmd("client {} place bid 200 on {}".format(timothy, auctionId))
    waitRequestSuccess(cli, nodeCount * 9)

    cli.enterCmd("client {} balance".format(timothy))
    waitRequestSuccess(cli, nodeCount * 10)
    waitBalanceChange(cli, nodeCount, 800)

    cli.enterCmd("client {} balance".format(les))
    waitRequestSuccess(cli, nodeCount * 11)
    waitBalanceChange(cli, nodeCount * 3, 1000)

    cli.enterCmd("client {} end auction {}".format(jason, auctionId))
    waitRequestSuccess(cli, nodeCount * 12)

    cli.enterCmd("client {} place bid 300 on {}".format(john, auctionId))
    waitRequestSuccess(cli, nodeCount * 12)

    cli.enterCmd("client {} balance".format(john))
    waitRequestSuccess(cli, nodeCount * 13)
    waitBalanceChange(cli, nodeCount * 4, 1000)

    cli.enterCmd("client {} balance".format(tyler))
    waitRequestSuccess(cli, nodeCount * 14)
    waitBalanceChange(cli, nodeCount * 5, 1000)

    cli.enterCmd("client {} balance".format(john))
    waitRequestSuccess(cli, nodeCount * 15)
    waitBalanceChange(cli, nodeCount * 6, 1000)

    cli.enterCmd("client {} balance".format(les))
    waitRequestSuccess(cli, nodeCount * 16)
    waitBalanceChange(cli, nodeCount * 7, 1000)

    cli.enterCmd("client {} balance".format(timothy))
    waitRequestSuccess(cli, nodeCount * 17)
    waitBalanceChange(cli, nodeCount * 2, 800)
