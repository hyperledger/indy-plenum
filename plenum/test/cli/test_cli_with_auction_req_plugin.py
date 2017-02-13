from functools import partial
from uuid import uuid4

import pytest

from plenum.common.eventually import eventually
from plenum.test.cli.helper import loadPlugin, assertNoClient, \
    createClientAndConnect, checkReply, checkSuccess, checkBalance


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


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def testTransactions(cli, loadAuctionReqPlugin, createAllNodes, validNodeNames):
    nodeCount = len(validNodeNames)
    auctionId = str(uuid4())
    names = ["Jason", "John", "Les", "Timothy", "Tyler"]
    for name in names:
        createClientAndConnect(cli, validNodeNames, name)
    jason, tyler, les, john, timothy = names
    cli.enterCmd("client {} start auction {}".format(jason, auctionId))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 1,
                              checkSuccess, retryWait=1, timeout=5))
    cli.enterCmd("client {} place bid 20 on {}".format(tyler, auctionId))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 2,
                              checkSuccess, retryWait=1, timeout=5))
    cli.enterCmd("client {} balance".format(tyler))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 3,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount,
                              partial(checkBalance, 980), retryWait=1,
                              timeout=5))
    cli.enterCmd("client {} place bid 40 on {}".format(les, auctionId))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 4,
                              checkSuccess, retryWait=1, timeout=5))
    cli.enterCmd("client {} balance".format(tyler))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 5,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 1,
                              partial(checkBalance, 1000), retryWait=1,
                              timeout=5))
    cli.enterCmd("client {} balance".format(les))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 6,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount,
                              partial(checkBalance, 960), retryWait=1,
                              timeout=5))

    # This bid would fail so `success` would be false and thus success count
    # wont increase
    cli.enterCmd("client {} place bid 30 on {}".format(john, auctionId))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 6,
                              checkSuccess, retryWait=1, timeout=5))
    cli.enterCmd("client {} balance".format(john))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 7,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 2,
                              partial(checkBalance, 1000), retryWait=1,
                              timeout=5))
    cli.enterCmd("client {} balance".format(les))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 8,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 2,
                              partial(checkBalance, 960), retryWait=1,
                              timeout=5))
    cli.enterCmd("client {} place bid 200 on {}".format(timothy, auctionId))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 9,
                              checkSuccess, retryWait=1, timeout=5))
    cli.enterCmd("client {} balance".format(timothy))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 10,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount,
                              partial(checkBalance, 800), retryWait=1,
                              timeout=5))
    cli.enterCmd("client {} balance".format(les))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 11,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 3,
                              partial(checkBalance, 1000), retryWait=1,
                              timeout=5))
    cli.enterCmd("client {} end auction {}".format(jason, auctionId))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 12,
                              checkSuccess, retryWait=1, timeout=5))
    cli.enterCmd("client {} place bid 300 on {}".format(john, auctionId))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 12,
                              checkSuccess, retryWait=1, timeout=5))
    cli.enterCmd("client {} balance".format(john))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 13,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 4,
                              partial(checkBalance, 1000), retryWait=1,
                              timeout=5))
    cli.enterCmd("client {} balance".format(tyler))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 14,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 5,
                              partial(checkBalance, 1000), retryWait=1,
                              timeout=5))
    cli.enterCmd("client {} balance".format(john))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 15,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 6,
                              partial(checkBalance, 1000), retryWait=1,
                              timeout=5))
    cli.enterCmd("client {} balance".format(les))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 16,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 7,
                              partial(checkBalance, 1000), retryWait=1,
                              timeout=5))
    cli.enterCmd("client {} balance".format(timothy))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 17,
                              checkSuccess, retryWait=1, timeout=5))
    cli.looper.run(eventually(checkReply, cli, nodeCount * 2,
                              partial(checkBalance, 800), retryWait=1,
                              timeout=5))
