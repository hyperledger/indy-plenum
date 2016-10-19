import inspect
import logging
import json
import os
from functools import partial
from typing import Dict, Any
import itertools

import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.serializers.compact_serializer import CompactSerializer
from plenum.common.exceptions import BlowUp

from plenum.common.looper import Looper
from plenum.common.raet import initLocalKeep
from plenum.common.txn import TXN_TYPE, DATA, NEW_NODE, ALIAS, CLIENT_PORT, \
    CLIENT_IP, NODE_PORT, CHANGE_HA, CHANGE_KEYS, NYM
from plenum.common.types import HA, CLIENT_STACK_SUFFIX, PLUGIN_BASE_DIR_PATH, \
    PLUGIN_TYPE_STATS_CONSUMER
from plenum.common.util import getNoInstances, getConfig
from plenum.common.port_dispenser import genHa
from plenum.common.log import getlogger, TestingHandler
from plenum.common.txn_util import getTxnOrderedFields
from plenum.test.eventually import eventually, eventuallyAll
from plenum.test.helper import TestNodeSet, genNodeReg, Pool, \
    ensureElectionsDone, checkNodesConnected, genTestClient, randomOperation, \
    checkReqAck, checkLastClientReqForNode, getPrimaryReplica, \
    checkRequestReturnedToNode, \
    checkSufficientRepliesRecvd, checkViewNoForNodes, TestNode
from plenum.test.node_request.node_request_helper import checkPrePrepared, \
    checkPropagated, checkPrepared, checkCommited
from plenum.test.plugin.helper import getPluginPath

logger = getlogger()


def getValueFromModule(request, name: str, default: Any = None):
    """
    Gets an attribute from the request's module if attribute is found
    else return the default value

    :param request:
    :param name: name of attribute to get from module
    :param default: value to return if attribute was not found
    :return: value of the attribute if attribute was found in module else the default value
    """
    if hasattr(request.module, name):
        value = getattr(request.module, name)
        logger.info("found {} in the module: {}".
                     format(name, value))
    else:
        value = default if default is not None else None
        logger.info("no {} found in the module, using the default: {}".
                     format(name, value))
    return value


basePath = os.path.dirname(os.path.abspath(__file__))
testPluginBaseDirPath = os.path.join(basePath, "plugin")

overriddenConfigValues = {
    "DefaultPluginPath": {
        PLUGIN_BASE_DIR_PATH: testPluginBaseDirPath,
        PLUGIN_TYPE_STATS_CONSUMER: "stats_consumer"
    }
}


@pytest.fixture(scope="module")
def allPluginsPath():
    return [getPluginPath('stats_consumer')]


@pytest.fixture(scope="module")
def keySharedNodes(startedNodes):
    for n in startedNodes:
        n.startKeySharing()
    return startedNodes


@pytest.fixture(scope="module")
def startedNodes(nodeSet, looper):
    for n in nodeSet:
        n.start(looper.loop)
    return nodeSet


@pytest.fixture(scope="module")
def whitelist(request):
    return getValueFromModule(request, "whitelist", [])


@pytest.fixture(scope="module")
def concerningLogLevels(request):
    # TODO need to enable WARNING for all tests
    default = [#logging.WARNING,
               logging.ERROR,
               logging.CRITICAL]
    return getValueFromModule(request, "concerningLogLevels", default)


@pytest.fixture(scope="function", autouse=True)
def logcapture(request, whitelist, concerningLogLevels):
    baseWhitelist = ['seconds to run once nicely',
                     'Executing %s took %.3f seconds',
                     'is already stopped',
                     'Error while running coroutine',
                     # TODO: This is too specific, move it to the particular test
                     "Beta discarding message INSTANCE_CHANGE(viewNo='BAD') "
                     "because field viewNo has incorrect type: <class 'str'>"
                     ]
    wlfunc = inspect.isfunction(whitelist)

    def tester(record):
        isBenign = record.levelno not in concerningLogLevels
        # TODO is this sufficient to test if a log is from test or not?
        isTest = os.path.sep + 'test' in record.pathname

        if wlfunc:
            wl = whitelist()
        else:
            wl = whitelist

        whiteListedExceptions = baseWhitelist + wl
        isWhiteListed = bool([w for w in whiteListedExceptions
                              if w in record.msg])
        if not (isBenign or isTest or isWhiteListed):
            raise BlowUp("{}: {} ".format(record.levelname, record.msg))

    ch = TestingHandler(tester)
    logging.getLogger().addHandler(ch)

    request.addfinalizer(lambda: logging.getLogger().removeHandler(ch))
    config = getConfig(tdir)
    for k, v in overriddenConfigValues.items():
        setattr(config, k, v)


@pytest.yield_fixture(scope="module")
def nodeSet(request, tdir, nodeReg, allPluginsPath):
    primaryDecider = getValueFromModule(request, "PrimaryDecider", None)
    with TestNodeSet(nodeReg=nodeReg, tmpdir=tdir,
                     primaryDecider=primaryDecider,
                     pluginPaths=allPluginsPath) as ns:
        yield ns


@pytest.fixture(scope="session")
def counter():
    return itertools.count()


@pytest.fixture(scope='module')
def tdir(tmpdir_factory, counter):
    tempdir = os.path.join(tmpdir_factory.getbasetemp().strpath,
                           str(next(counter)))
    logger.debug("module-level temporary directory: {}".format(tempdir))
    return tempdir


@pytest.fixture(scope='function')
def tdir_for_func(tmpdir_factory, counter):
    tempdir = os.path.join(tmpdir_factory.getbasetemp().strpath,
                           str(next(counter)))
    logging.debug("function-level temporary directory: {}".format(tempdir))
    return tempdir


@pytest.fixture(scope="module")
def nodeReg(request) -> Dict[str, HA]:
    nodeCount = getValueFromModule(request, "nodeCount", 4)
    return genNodeReg(count=nodeCount)


@pytest.yield_fixture(scope="module")
def unstartedLooper(nodeSet):
    with Looper(nodeSet, autoStart=False) as l:
        yield l


@pytest.fixture(scope="module")
def looper(unstartedLooper):
    unstartedLooper.autoStart = True
    unstartedLooper.startall()
    return unstartedLooper


@pytest.fixture(scope="module")
def pool(tmpdir_factory, counter):
    return Pool(tmpdir_factory, counter)


@pytest.fixture(scope="module")
def ready(looper, keySharedNodes):
    looper.run(checkNodesConnected(keySharedNodes))
    return keySharedNodes


@pytest.fixture(scope="module")
def up(looper, ready):
    ensureElectionsDone(looper=looper, nodes=ready, retryWait=1, timeout=30)


# noinspection PyIncorrectDocstring
@pytest.fixture(scope="module")
def ensureView(nodeSet, looper, up):
    """
    Ensure that all the nodes in the nodeSet are in the same view.
    """
    return looper.run(eventually(checkViewNoForNodes, nodeSet, timeout=3))


@pytest.fixture("module")
def delayedPerf(nodeSet):
    for node in nodeSet:
        node.delayCheckPerformance(20)


@pytest.fixture(scope="module")
def clientAndWallet1(looper, nodeSet, tdir, up):
    return genTestClient(nodeSet, tmpdir=tdir)


@pytest.fixture(scope="module")
def client1(clientAndWallet1, looper):
    client, _ = clientAndWallet1
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())
    return client


@pytest.fixture(scope="module")
def wallet1(clientAndWallet1):
    _, wallet = clientAndWallet1
    return wallet


@pytest.fixture(scope="module")
def request1(wallet1):
    op = randomOperation()
    req = wallet1.signOp(op)
    return req


@pytest.fixture(scope="module")
def sent1(client1, request1):
    return client1.submitReqs(request1)[0]


@pytest.fixture(scope="module")
def reqAcked1(looper, nodeSet, client1, sent1, faultyNodes):
    coros = [partial(checkLastClientReqForNode, node, sent1)
             for node in nodeSet]
    looper.run(eventuallyAll(*coros,
                             totalTimeout=10,
                             acceptableFails=faultyNodes))

    coros2 = [partial(checkReqAck, client1, node, sent1.reqId)
              for node in nodeSet]
    looper.run(eventuallyAll(*coros2,
                             totalTimeout=5,
                             acceptableFails=faultyNodes))

    return sent1


@pytest.fixture(scope="module")
def faultyNodes(request):
    return getValueFromModule(request, "faultyNodes", 0)


@pytest.fixture(scope="module")
def propagated1(looper,
                nodeSet,
                up,
                reqAcked1,
                faultyNodes):
    checkPropagated(looper, nodeSet, reqAcked1, faultyNodes)
    return reqAcked1


@pytest.fixture(scope="module")
def preprepared1(looper, nodeSet, propagated1, faultyNodes):
    checkPrePrepared(looper,
                     nodeSet,
                     propagated1,
                     range(getNoInstances(len(nodeSet))),
                     faultyNodes)
    return propagated1


@pytest.fixture(scope="module")
def prepared1(looper, nodeSet, client1, preprepared1, faultyNodes):
    checkPrepared(looper,
                  nodeSet,
                  preprepared1,
                  range(getNoInstances(len(nodeSet))),
                  faultyNodes)
    return preprepared1


@pytest.fixture(scope="module")
def committed1(looper, nodeSet, client1, prepared1, faultyNodes):
    checkCommited(looper,
                  nodeSet,
                  prepared1,
                  range(getNoInstances(len(nodeSet))),
                  faultyNodes)
    return prepared1


@pytest.fixture(scope="module")
def replied1(looper, nodeSet, client1, committed1, wallet1):
    for instId in range(getNoInstances(len(nodeSet))):
        getPrimaryReplica(nodeSet, instId)

        looper.run(*[eventually(checkRequestReturnedToNode,
                                node,
                                wallet1.defaultId,
                                committed1.reqId,
                                committed1.digest,
                                instId,
                                retryWait=1, timeout=30)
                     for node in nodeSet])

        looper.run(eventually(
                checkSufficientRepliesRecvd,
                client1.inBox,
                committed1.reqId,
                2,
                retryWait=2,
                timeout=30))
    return committed1


@pytest.yield_fixture(scope="module")
def looperWithoutNodeSet():
    with Looper(debug=True) as looper:
        yield looper


@pytest.fixture(scope="module")
def poolTxnNodeNames():
    return "Alpha", "Beta", "Gamma", "Delta"


@pytest.fixture(scope="module")
def poolTxnClientNames():
    return "Alice", "Jason", "John", "Les"


@pytest.fixture(scope="module")
def poolTxnStewardNames():
    return "Steward1", "Steward2", "Steward3", "Steward4"


@pytest.fixture(scope="module")
def conf(tdir):
    return getConfig(tdir)


# TODO: This fixture is probably not needed now, as getConfig takes the
# `baseDir`. Confirm and remove
@pytest.fixture(scope="module")
def tconf(conf, tdir):
    conf.baseDir = tdir
    return conf


@pytest.fixture(scope="module")
def dirName():
    return os.path.dirname


@pytest.fixture(scope="module")
def poolTxnData(dirName):
    filePath = os.path.join(dirName(__file__), "node_and_client_info.py")
    data = json.loads(open(filePath).read().strip())
    for txn in data["txns"]:
        if txn[TXN_TYPE] == NEW_NODE:
            txn[DATA][NODE_PORT] = genHa()[1]
            txn[DATA][CLIENT_PORT] = genHa()[1]
    return data


@pytest.fixture(scope="module")
def tdirWithPoolTxns(poolTxnData, tdir, tconf):
    ledger = Ledger(CompactMerkleTree(),
                    dataDir=tdir,
                    fileName=tconf.poolTransactionsFile)
    for item in poolTxnData["txns"]:
        if item.get(TXN_TYPE) in (NEW_NODE, CHANGE_HA, CHANGE_KEYS):
            ledger.add(item)
    return tdir


@pytest.fixture(scope="module")
def domainTxnOrderedFields():
    return getTxnOrderedFields()


@pytest.fixture(scope="module")
def tdirWithDomainTxns(poolTxnData, tdir, tconf, domainTxnOrderedFields):
    ledger = Ledger(CompactMerkleTree(),
                    dataDir=tdir,
                    serializer=CompactSerializer(fields=domainTxnOrderedFields),
                    fileName=tconf.domainTransactionsFile)
    for item in poolTxnData["txns"]:
        if item.get(TXN_TYPE) == NYM:
            ledger.add(item)
    return tdir


@pytest.fixture(scope="module")
def tdirWithNodeKeepInited(tdir, poolTxnData, poolTxnNodeNames):
    seeds = poolTxnData["seeds"]
    for nName in poolTxnNodeNames:
        initLocalKeep(nName, tdir, seeds[nName], override=True)


@pytest.fixture(scope="module")
def poolTxnClientData(poolTxnClientNames, poolTxnData):
    name = poolTxnClientNames[0]
    seed = poolTxnData["seeds"][name]
    return name, seed.encode()


@pytest.fixture(scope="module")
def poolTxnStewardData(poolTxnStewardNames, poolTxnData):
    name = poolTxnStewardNames[0]
    seed = poolTxnData["seeds"][name]
    return name, seed.encode()


@pytest.fixture(scope="module")
def poolTxnClient(tdirWithPoolTxns, tdirWithDomainTxns, txnPoolNodeSet):
    return genTestClient(txnPoolNodeSet, tmpdir=tdirWithPoolTxns,
                         usePoolLedger=True)


@pytest.fixture(scope="module")
def testNodeClass():
    return TestNode


@pytest.yield_fixture(scope="module")
def txnPoolNodeSet(tdirWithPoolTxns,
                   tdirWithDomainTxns,
                   tconf,
                   poolTxnNodeNames,
                   allPluginsPath,
                   tdirWithNodeKeepInited,
                   testNodeClass):
    with Looper(debug=True) as looper:
        nodes = []
        for nm in poolTxnNodeNames:
            node = testNodeClass(nm, basedirpath=tdirWithPoolTxns,
                            config=tconf, pluginPaths=allPluginsPath)
            looper.add(node)
            nodes.append(node)

        looper.run(checkNodesConnected(nodes))
        yield nodes


@pytest.fixture(scope="module")
def txnPoolCliNodeReg(poolTxnData):
    cliNodeReg = {}
    for txn in poolTxnData["txns"]:
        if txn[TXN_TYPE] == NEW_NODE:
            data = txn[DATA]
            cliNodeReg[data[ALIAS]+CLIENT_STACK_SUFFIX] = HA(data[CLIENT_IP],
                                                             data[CLIENT_PORT])
    return cliNodeReg


@pytest.fixture(scope="module")
def postingStatsEnabled(request):
    config = getConfig()
    config.SendMonitorStats = True

    def reset():
        config.SendMonitorStats = False

    request.addfinalizer(reset)
