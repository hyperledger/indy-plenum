import pytest
from plenum.common.looper import Looper
from plenum.common.port_dispenser import genHa
from plenum.common.script_helper import changeHACore
from plenum.common.signer_simple import SimpleSigner

from plenum.common.util import getMaxFailures
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd


@pytest.yield_fixture(scope="module")
def looper():
    with Looper() as l:
        yield l


def testChangeNodeHa(looper, txnPoolNodeSet, tdir, poolTxnData,
                     poolTxnStewardData, conf):
    nodeName = txnPoolNodeSet[0].name
    nodeSeed = poolTxnData["seeds"][nodeName]
    nodeVerkey = SimpleSigner(seed=bytes(nodeSeed, 'utf-8')).verkey
    _, stewardsSeed = poolTxnStewardData
    ip, port = genHa()
    nodeStackNewHA = "{}:{}".format(ip, port)

    client, req = changeHACore(looper, tdir, conf, "test", nodeName,
                               nodeVerkey, stewardsSeed, nodeStackNewHA)
    f = getMaxFailures(len(client.nodeReg))
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox, req.reqId
                          , f, retryWait=1, timeout=8))
    looper.runFor(10)

    # TODO: Once it is sure, that node ha is changed, following is pending
    # 1. Restart nodes with new node ha
    # 2. Start a new client (should have different tdir)
    # with pool txn files created there, and have it connect to those nodes
    # 3. Check that client's master pool txn file
    # gets updated (corresponding code needs to be written)
    # 4. Any other tests we can think of to thoroughly test it
