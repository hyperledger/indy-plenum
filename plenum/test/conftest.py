import importlib
import inspect
import itertools
import logging
import os
import shutil
import re
import warnings
import json
from contextlib import ExitStack
from copy import copy
from functools import partial
import time
from typing import Dict, Any

from indy.pool import create_pool_ledger_config, open_pool_ledger, close_pool_ledger
from indy.wallet import create_wallet, open_wallet, close_wallet
from indy.did import create_and_store_my_did

from ledger.genesis_txn.genesis_txn_file_util import create_genesis_txn_init_ledger
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.common.signer_did import DidSigner
from plenum.common.signer_simple import SimpleSigner
from plenum.test import waits

import gc
import pip
import pytest
import plenum.config as plenum_config
import plenum.server.general_config.ubuntu_platform_config as platform_config
from plenum.common.keygen_utils import initNodeKeysForBothStacks, init_bls_keys
from plenum.test.greek import genNodeNames
from plenum.test.grouped_load_scheduling import GroupedLoadScheduling
from plenum.test.node_catchup.helper import ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet, sdk_add_new_nym
from stp_core.common.logging.handlers import TestingHandler
from stp_core.crypto.util import randomSeed
from stp_core.network.port_dispenser import genHa
from stp_core.types import HA
from _pytest.recwarn import WarningsRecorder

from plenum.common.config_util import getConfig
from stp_core.loop.eventually import eventually
from plenum.common.exceptions import BlowUp
from stp_core.common.log import getlogger, Logger
from stp_core.loop.looper import Looper, Prodable
from plenum.common.constants import TXN_TYPE, DATA, NODE, ALIAS, CLIENT_PORT, \
    CLIENT_IP, NODE_PORT, NYM, CLIENT_STACK_SUFFIX, PLUGIN_BASE_DIR_PATH, ROLE, \
    STEWARD, TARGET_NYM, VALIDATOR, SERVICES, NODE_IP, BLS_KEY, VERKEY, TRUSTEE
from plenum.common.txn_util import getTxnOrderedFields
from plenum.common.types import PLUGIN_TYPE_STATS_CONSUMER, f
from plenum.common.util import getNoInstances
from plenum.server.notifier_plugin_manager import PluginManager
from plenum.test.helper import randomOperation, \
    checkReqAck, checkLastClientReqForNode, waitForSufficientRepliesForRequests, \
    waitForViewChange, requestReturnedToNode, randomText, \
    mockGetInstalledDistributions, mockImportModule, chk_all_funcs, \
    create_new_test_node
from plenum.test.node_request.node_request_helper import checkPrePrepared, \
    checkPropagated, checkPrepared, checkCommitted
from plenum.test.plugin.helper import getPluginPath
from plenum.test.test_client import genTestClient, TestClient
from plenum.test.test_node import TestNode, TestNodeSet, Pool, \
    checkNodesConnected, ensureElectionsDone, genNodeReg
from plenum.common.config_helper import PConfigHelper, PNodeConfigHelper

Logger.setLogLevel(logging.NOTSET)
logger = getlogger()

GENERAL_CONFIG_DIR = 'etc/indy'


def get_data_for_role(pool_txn_data, role):
    name_and_seeds = []
    for txn in pool_txn_data['txns']:
        if txn.get(ROLE) == role:
            name = txn[ALIAS]
            name_and_seeds.append((name, pool_txn_data['seeds'][name]))
    return name_and_seeds


@pytest.mark.firstresult
def pytest_xdist_make_scheduler(config, log):
    return GroupedLoadScheduling(config, log)


@pytest.fixture(scope="session")
def warnfilters():
    def _():
        warnings.filterwarnings(
            'ignore',
            category=DeprecationWarning,
            module='jsonpickle\.pickler',
            message='encodestring\(\) is a deprecated alias')
        warnings.filterwarnings(
            'ignore',
            category=DeprecationWarning,
            module='jsonpickle\.unpickler',
            message='decodestring\(\) is a deprecated alias')
        warnings.filterwarnings(
            'ignore',
            category=DeprecationWarning,
            module='plenum\.client\.client',
            message="The 'warn' method is deprecated")
        warnings.filterwarnings(
            'ignore',
            category=DeprecationWarning,
            module='plenum\.common\.stacked',
            message="The 'warn' method is deprecated")
        warnings.filterwarnings(
            'ignore',
            category=DeprecationWarning,
            module='plenum\.test\.test_testable',
            message='Please use assertEqual instead.')
        warnings.filterwarnings(
            'ignore',
            category=DeprecationWarning,
            module='prompt_toolkit\.filters\.base',
            message='inspect\.getargspec\(\) is deprecated')
        warnings.filterwarnings(
            'ignore', category=ResourceWarning, message='unclosed event loop')
        warnings.filterwarnings(
            'ignore',
            category=ResourceWarning,
            message='unclosed.*socket\.socket')

    return _


@pytest.yield_fixture(scope="session", autouse=True)
def warncheck(warnfilters):
    with WarningsRecorder() as record:
        warnfilters()
        yield
        gc.collect()
    to_prints = []

    def keyfunc(_):
        return _.category.__name__, _.filename, _.lineno

    _sorted = sorted(record, key=keyfunc)
    _grouped = itertools.groupby(_sorted, keyfunc)
    for k, g in _grouped:
        to_prints.append("\n"
                         "category: {}\n"
                         "filename: {}\n"
                         "  lineno: {}".format(*k))
        messages = itertools.groupby(g, lambda _: str(_.message))
        for k2, g2 in messages:
            count = sum(1 for _ in g2)
            count_str = ' ({} times)'.format(count) if count > 1 else ''
            to_prints.append("     msg: {}{}".format(k2, count_str))
    if to_prints:
        to_prints.insert(0, 'Warnings found:')
        pytest.fail('\n'.join(to_prints))


@pytest.fixture(scope="function", autouse=True)
def limitTestRunningTime(request):
    st = time.time()
    yield
    runningTime = time.time() - st
    time_limit = getValueFromModule(request, "TestRunningTimeLimitSec",
                                    plenum_config.TestRunningTimeLimitSec)
    if runningTime > time_limit:
        pytest.fail(
            'The running time of each test is limited by {} sec '
            '(actually the test has taken {:2.1f} sec).\n'
            'In order to make the test passed there are two options:\n'
            '\t1. Make the test faster (for example: override default '
            'timeouts ONLY for the tests, do not wait '
            '`with pytest.raises(..)` and so on)\n'
            '\t2. Override the `limitTestRunningTime` fixture '
            'for the test module.\n'
            'Firstly, try to use the option #1.'
            ''.format(plenum_config.TestRunningTimeLimitSec, runningTime))


@pytest.fixture(scope="session", autouse=True)
def setResourceLimits():
    try:
        import resource
    except ImportError:
        print('Module resource is not available, maybe i am running on Windows')
        return
    flimit = 65535
    plimit = 65535
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (flimit, flimit))
        resource.setrlimit(resource.RLIMIT_NPROC, (plimit, plimit))
    except Exception as ex:
        print('Could not set resource limits due to {}'.format(ex))


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
    default = [  # logging.WARNING,
        logging.ERROR,
        logging.CRITICAL]
    return getValueFromModule(request, "concerningLogLevels", default)


@pytest.fixture(scope="function", autouse=True)
def logcapture(request, whitelist, concerningLogLevels):
    baseWhitelist = ['seconds to run once nicely',
                     'Executing %s took %.3f seconds',
                     'is already stopped',
                     'Error while running coroutine',
                     'not trying any more because',
                     # TODO: This is too specific, move it to the particular
                     # test
                     "Beta discarding message INSTANCE_CHANGE(viewNo='BAD') "
                     "because field viewNo has incorrect type: <class 'str'>",
                     'got exception while closing hash store',
                     # TODO: Remove these once the relevant bugs are fixed
                     '.+ failed to ping .+ at',
                     'discarding message (NOMINATE|PRIMARY)',
                     '.+ rid .+ has been removed',
                     'last try...',
                     'has uninitialised socket',
                     'to have incorrect time',
                     'time not acceptable'
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

        # Converting the log message to its string representation, the log
        # message can be an arbitrary object
        if not (isBenign or isTest):
            msg = str(record.msg)
            # TODO combine whitelisted with '|' and use one regex for msg
            isWhiteListed = any(re.search(w, msg)
                                for w in whiteListedExceptions)
            if not isWhiteListed:
                # Stopping all loopers, so prodables like nodes, clients, etc stop.
                #  This helps in freeing ports
                for fv in request._fixture_values.values():
                    if isinstance(fv, Looper):
                        fv.stopall()
                    if isinstance(fv, Prodable):
                        fv.stop()
                raise BlowUp("{}: {} ".format(record.levelname, record.msg))

    ch = TestingHandler(tester)
    logging.getLogger().addHandler(ch)

    def cleanup():
        logging.getLogger().removeHandler(ch)

    request.addfinalizer(cleanup)


@pytest.fixture(scope="module")
def config_helper_class():
    return PConfigHelper


@pytest.fixture(scope="module")
def node_config_helper_class():
    return PNodeConfigHelper


@pytest.yield_fixture(scope="module")
def nodeSet(request, tdir, tconf, nodeReg, allPluginsPath, patchPluginManager):
    primaryDecider = getValueFromModule(request, "PrimaryDecider", None)
    with TestNodeSet(tconf, nodeReg=nodeReg, tmpdir=tdir,
                     primaryDecider=primaryDecider,
                     pluginPaths=allPluginsPath) as ns:
        yield ns


def _tdir(tdir_fact):
    return tdir_fact.mktemp('').strpath


@pytest.fixture(scope='module')
def tdir(tmpdir_factory):
    tempdir = _tdir(tmpdir_factory)
    logger.debug("module-level temporary directory: {}".format(tempdir))
    return tempdir


@pytest.fixture()
def tdir_for_func(tmpdir_factory):
    tempdir = _tdir(tmpdir_factory)
    logging.debug("function-level temporary directory: {}".format(tempdir))
    return tempdir


def _client_tdir(temp_dir):
    path = os.path.join(temp_dir, "home", "testuser")
    os.makedirs(path)
    return path


@pytest.fixture(scope='module')
def client_tdir(tdir):
    tempdir = _client_tdir(tdir)
    logger.debug("module-level client temporary directory: {}".format(tempdir))
    return tempdir


@pytest.fixture(scope='function')
def client_tdir_for_func(tdir_for_func):
    tempdir = _client_tdir(tdir_for_func)
    logger.debug("function-level client temporary directory: {}".format(tempdir))
    return tempdir


def _general_conf_tdir(tmp_dir):
    general_config_dir = os.path.join(tmp_dir, GENERAL_CONFIG_DIR)
    os.makedirs(general_config_dir)
    general_config = os.path.join(general_config_dir, plenum_config.GENERAL_CONFIG_FILE)
    shutil.copy(platform_config.__file__, general_config)
    return general_config_dir


@pytest.fixture(scope='module')
def general_conf_tdir(tdir):
    general_config_dir = _general_conf_tdir(tdir)
    logger.debug("module-level general config directory: {}".format(general_config_dir))
    return general_config_dir


@pytest.fixture()
def general_conf_tdir_for_func(tdir_for_func):
    general_config_dir = _general_conf_tdir(tdir_for_func)
    logger.debug("function-level general config directory: {}".format(general_config_dir))
    return general_config_dir


def _tconf(general_config):
    config = getConfig(general_config)
    for k, v in overriddenConfigValues.items():
        setattr(config, k, v)
    return config


@pytest.fixture(scope="module")
def tconf(general_conf_tdir):
    return _tconf(general_conf_tdir)


@pytest.fixture()
def tconf_for_func(general_conf_tdir_for_func):
    return _tconf(general_conf_tdir_for_func)


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


@pytest.fixture(scope="function")
def pool(tdir_for_func, tconf_for_func):
    return Pool(tmpdir=tdir_for_func, config=tconf_for_func)


@pytest.fixture(scope="module")
def ready(looper, keySharedNodes):
    looper.run(checkNodesConnected(keySharedNodes))
    return keySharedNodes


@pytest.fixture(scope="module")
def up(looper, ready):
    ensureElectionsDone(looper=looper, nodes=ready)


# noinspection PyIncorrectDocstring
@pytest.fixture(scope="module")
def ensureView(nodeSet, looper, up):
    """
    Ensure that all the nodes in the nodeSet are in the same view.
    """
    return waitForViewChange(looper, nodeSet)


@pytest.fixture("module")
def delayed_perf_chk(nodeSet):
    d = 20
    for node in nodeSet:
        node.delayCheckPerformance(d)
    return d


@pytest.fixture(scope="module")
def clientAndWallet1(looper, nodeSet, client_tdir, up):
    client, wallet = genTestClient(nodeSet, tmpdir=client_tdir)
    yield client, wallet
    client.stop()


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
    return client1.submitReqs(request1)[0][0]


@pytest.fixture(scope="module")
def reqAcked1(looper, nodeSet, client1, sent1, faultyNodes):
    numerOfNodes = len(nodeSet)

    # Wait until request received by all nodes
    propTimeout = waits.expectedClientToPoolRequestDeliveryTime(numerOfNodes)
    coros = [partial(checkLastClientReqForNode, node, sent1)
             for node in nodeSet]
    # looper.run(eventuallyAll(*coros,
    #                          totalTimeout=propTimeout,
    #                          acceptableFails=faultyNodes))
    chk_all_funcs(looper, coros, acceptable_fails=faultyNodes,
                  timeout=propTimeout)

    # Wait until sufficient number of acks received
    coros2 = [
        partial(
            checkReqAck,
            client1,
            node,
            sent1.identifier,
            sent1.reqId) for node in nodeSet]
    ackTimeout = waits.expectedReqAckQuorumTime()
    # looper.run(eventuallyAll(*coros2,
    #                          totalTimeout=ackTimeout,
    #                          acceptableFails=faultyNodes))
    chk_all_funcs(looper, coros2, acceptable_fails=faultyNodes,
                  timeout=ackTimeout)
    return sent1


@pytest.fixture(scope="module")
def noRetryReq(tconf, request):
    oldRetryAck = tconf.CLIENT_MAX_RETRY_ACK
    oldRetryReply = tconf.CLIENT_MAX_RETRY_REPLY
    tconf.CLIENT_MAX_RETRY_ACK = 0
    tconf.CLIENT_MAX_RETRY_REPLY = 0

    def reset():
        tconf.CLIENT_MAX_RETRY_ACK = oldRetryAck
        tconf.CLIENT_MAX_RETRY_REPLY = oldRetryReply

    request.addfinalizer(reset)
    return tconf


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
    checkCommitted(looper,
                   nodeSet,
                   prepared1,
                   range(getNoInstances(len(nodeSet))),
                   faultyNodes)
    return prepared1


@pytest.fixture(scope="module")
def replied1(looper, nodeSet, client1, committed1, wallet1, faultyNodes):
    numOfNodes = len(nodeSet)
    numOfInstances = getNoInstances(numOfNodes)
    quorum = numOfInstances * (numOfNodes - faultyNodes)

    def checkOrderedCount():
        resp = [requestReturnedToNode(node,
                                      wallet1.defaultId,
                                      committed1.reqId,
                                      instId)
                for node in nodeSet for instId in range(numOfInstances)]
        assert resp.count(True) >= quorum

    orderingTimeout = waits.expectedOrderingTime(numOfInstances)
    looper.run(eventually(checkOrderedCount,
                          retryWait=1,
                          timeout=orderingTimeout))

    waitForSufficientRepliesForRequests(looper, client1, requests=[committed1])
    return committed1


@pytest.yield_fixture(scope="module")
def looperWithoutNodeSet():
    with Looper() as looper:
        yield looper


@pytest.yield_fixture()
def looper_without_nodeset_for_func():
    with Looper() as looper:
        yield looper


@pytest.fixture(scope="module")
def poolTxnNodeNames(request, index=""):
    nodeCount = getValueFromModule(request, "nodeCount", 4)
    return [n + index for n in genNodeNames(nodeCount)]


@pytest.fixture(scope="module")
def poolTxnClientNames():
    return "Alice", "Jason", "John", "Les"


@pytest.fixture(scope="module")
def poolTxnStewardNames(request):
    nodeCount = getValueFromModule(request, "nodeCount", 4)
    return ['Steward' + str(i) for i in range(1, nodeCount + 1)]


@pytest.fixture(scope="module")
def dirName():
    return os.path.dirname


@pytest.fixture(scope="module")
def poolTxnData(request):
    nodeCount = getValueFromModule(request, "nodeCount", 4)
    nodes_with_bls = getValueFromModule(request, "nodes_wth_bls", nodeCount)
    data = {'txns': [], 'seeds': {}, 'nodesWithBls': {}}
    for i, node_name in zip(range(1, nodeCount + 1), genNodeNames(nodeCount)):
        data['seeds'][node_name] = node_name + '0' * (32 - len(node_name))
        steward_name = 'Steward' + str(i)
        data['seeds'][steward_name] = steward_name + \
                                      '0' * (32 - len(steward_name))

        n_idr = SimpleSigner(seed=data['seeds'][node_name].encode()).identifier
        s_idr = DidSigner(seed=data['seeds'][steward_name].encode())

        data['txns'].append({
            TXN_TYPE: NYM,
            ROLE: STEWARD,
            ALIAS: steward_name,
            TARGET_NYM: s_idr.identifier,
            VERKEY: s_idr.verkey,
        })
        node_txn = {
            TXN_TYPE: NODE,
            f.IDENTIFIER.nm: s_idr.identifier,
            TARGET_NYM: n_idr,
            DATA: {
                ALIAS: node_name,
                SERVICES: [VALIDATOR],
                NODE_IP: '127.0.0.1',
                NODE_PORT: genHa()[1],
                CLIENT_IP: '127.0.0.1',
                CLIENT_PORT: genHa()[1],
            }
        }

        if i <= nodes_with_bls:
            _, bls_key = create_default_bls_crypto_factory().generate_bls_keys(
                seed=data['seeds'][node_name])
            node_txn[DATA][BLS_KEY] = bls_key
            data['nodesWithBls'][node_name] = True

        data['txns'].append(node_txn)

    # Add 4 Trustees
    for i in range(4):
        trustee_name = 'Trs' + str(i)
        data['seeds'][trustee_name] = trustee_name + '0' * (
                32 - len(trustee_name))
        t_sgnr = DidSigner(seed=data['seeds'][trustee_name].encode())
        data['txns'].append({
            TXN_TYPE: NYM,
            ROLE: TRUSTEE,
            ALIAS: trustee_name,
            TARGET_NYM: t_sgnr.identifier,
            VERKEY: t_sgnr.verkey
        })

    more_data_seeds = \
        {
            "Alice": "99999999999999999999999999999999",
            "Jason": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "John": "dddddddddddddddddddddddddddddddd",
            "Les": "ffffffffffffffffffffffffffffffff"
        }
    more_data_users = []
    for more_name, more_seed in more_data_seeds.items():
        signer = DidSigner(seed=more_seed.encode())
        more_data_users.append({TXN_TYPE: NYM, ALIAS: more_name, TARGET_NYM: signer.identifier, VERKEY: signer.verkey,
                                f.IDENTIFIER.nm: "5rArie7XKukPCaEwq5XGQJnM9Fc5aZE3M9HAPVfMU2xC"})

    data['txns'].extend(more_data_users)
    data['seeds'].update(more_data_seeds)
    return data


@pytest.fixture(scope="module")
def tdirWithPoolTxns(config_helper_class, poolTxnData, tdir, tconf):
    import getpass
    logging.debug("current user when creating new pool txn file: {}".
                  format(getpass.getuser()))

    config_helper = config_helper_class(tconf, chroot=tdir)
    ledger = create_genesis_txn_init_ledger(config_helper.genesis_dir, tconf.poolTransactionsFile)

    for item in poolTxnData["txns"]:
        if item.get(TXN_TYPE) == NODE:
            ledger.add(item)
    ledger.stop()
    return config_helper.genesis_dir


@pytest.fixture(scope="module")
def client_ledger_dir(client_tdir):
    return client_tdir


@pytest.fixture(scope="module")
def tdirWithClientPoolTxns(poolTxnData, client_ledger_dir):
    import getpass
    logging.debug("current user when creating new pool txn file for client: {}".
                  format(getpass.getuser()))

    ledger = create_genesis_txn_init_ledger(client_ledger_dir, plenum_config.poolTransactionsFile)

    for item in poolTxnData["txns"]:
        if item.get(TXN_TYPE) == NODE:
            ledger.add(item)
    ledger.stop()
    return client_ledger_dir


@pytest.fixture(scope="module")
def domainTxnOrderedFields():
    return getTxnOrderedFields()


@pytest.fixture(scope="module")
def tdirWithDomainTxns(config_helper_class, poolTxnData, tdir, tconf, domainTxnOrderedFields):
    config_helper = config_helper_class(tconf, chroot=tdir)
    ledger = create_genesis_txn_init_ledger(config_helper.genesis_dir, tconf.domainTransactionsFile)

    for item in poolTxnData["txns"]:
        if item.get(TXN_TYPE) == NYM:
            ledger.add(item)
    ledger.stop()
    return config_helper.genesis_dir


@pytest.fixture(scope="module")
def tdirWithNodeKeepInited(tdir, tconf, node_config_helper_class, poolTxnData, poolTxnNodeNames):
    seeds = poolTxnData["seeds"]
    for nName in poolTxnNodeNames:
        seed = seeds[nName]
        use_bls = nName in poolTxnData['nodesWithBls']
        config_helper = node_config_helper_class(nName, tconf, chroot=tdir)
        initNodeKeysForBothStacks(nName, config_helper.keys_dir, seed, use_bls=use_bls, override=True)


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
def pool_txn_stewards_data(poolTxnStewardNames, poolTxnData):
    return [(name, poolTxnData["seeds"][name].encode())
            for name in poolTxnStewardNames]


@pytest.fixture(scope="module")
def trustee_data(poolTxnData):
    return get_data_for_role(poolTxnData, TRUSTEE)


@pytest.fixture(scope="module")
def stewards_and_wallets(looper, txnPoolNodeSet, pool_txn_stewards_data,
                         tdirWithClientPoolTxns):
    clients_and_wallets = []
    for pool_txn_steward_data in pool_txn_stewards_data:
        steward_client, steward_wallet = buildPoolClientAndWallet(pool_txn_steward_data,
                                                                  tdirWithClientPoolTxns)
        looper.add(steward_client)
        ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward_client,
                                                      *txnPoolNodeSet)
        clients_and_wallets.append((steward_client, steward_wallet))

    yield clients_and_wallets

    for (client, wallet) in clients_and_wallets:
        client.stop()


@pytest.fixture(scope="module")
def poolTxnClient(tdirWithClientPoolTxns, txnPoolNodeSet):
    return genTestClient(txnPoolNodeSet, tmpdir=tdirWithClientPoolTxns,
                         usePoolLedger=True)


@pytest.fixture(scope="module")
def testNodeClass(patchPluginManager):
    return TestNode


@pytest.fixture(scope="module")
def testClientClass():
    return TestClient


@pytest.yield_fixture(scope="module")
def txnPoolNodesLooper():
    with Looper() as l:
        yield l


@pytest.fixture(scope="module")
def do_post_node_creation():
    """
    This fixture is used to do any changes on the newly created node. To use
    this, override this fixture in test module or conftest and define the
    changes in the function `_post_node_creation`
    """

    def _post_node_creation(node):
        pass

    return _post_node_creation


@pytest.fixture(scope="module")
def txnPoolNodeSet(node_config_helper_class,
                   patchPluginManager,
                   txnPoolNodesLooper,
                   tdirWithPoolTxns,
                   tdirWithDomainTxns,
                   tdir,
                   tconf,
                   poolTxnNodeNames,
                   allPluginsPath,
                   tdirWithNodeKeepInited,
                   testNodeClass,
                   do_post_node_creation):
    with ExitStack() as exitStack:
        nodes = []
        for nm in poolTxnNodeNames:
            node = exitStack.enter_context(create_new_test_node(
                testNodeClass, node_config_helper_class, nm, tconf, tdir,
                allPluginsPath))
            do_post_node_creation(node)
            txnPoolNodesLooper.add(node)
            nodes.append(node)
        txnPoolNodesLooper.run(checkNodesConnected(nodes))
        ensureElectionsDone(looper=txnPoolNodesLooper, nodes=nodes)
        yield nodes


@pytest.fixture(scope="module")
def txnPoolCliNodeReg(poolTxnData):
    cliNodeReg = {}
    for txn in poolTxnData["txns"]:
        if txn[TXN_TYPE] == NODE:
            data = txn[DATA]
            cliNodeReg[data[ALIAS] +
                       CLIENT_STACK_SUFFIX] = HA(data[CLIENT_IP], data[CLIENT_PORT])
    return cliNodeReg


@pytest.fixture(scope="module")
def postingStatsEnabled(request, tconf):
    tconf.SendMonitorStats = True

    # def reset():
    #    config.SendMonitorStats = False

    # request.addfinalizer(reset)


@pytest.fixture
def pluginManager(monkeypatch):
    pluginManager = PluginManager()
    monkeypatch.setattr(importlib, 'import_module', mockImportModule)
    packagesCnt = 3
    packages = [pluginManager.prefix + randomText(10)
                for _ in range(packagesCnt)]
    monkeypatch.setattr(pip.utils, 'get_installed_distributions',
                        partial(mockGetInstalledDistributions,
                                packages=packages))
    imported, found = pluginManager.importPlugins()
    assert imported == 3
    assert hasattr(pluginManager, 'prefix')
    assert hasattr(pluginManager, '_sendMessage')
    assert hasattr(pluginManager, '_findPlugins')
    yield pluginManager
    monkeypatch.undo()


@pytest.fixture(scope="module")
def patchPluginManager():
    pluginManager = PluginManager()
    pluginManager.plugins = []
    return pluginManager


@pytest.fixture
def pluginManagerWithImportedModules(pluginManager, monkeypatch):
    monkeypatch.setattr(pip.utils, 'get_installed_distributions',
                        partial(mockGetInstalledDistributions,
                                packages=[]))
    monkeypatch.setattr(importlib, 'import_module', mockImportModule)
    imported, found = pluginManager.importPlugins()
    assert imported == 0
    packagesCnt = 3
    packages = [pluginManager.prefix + randomText(10)
                for _ in range(packagesCnt)]
    monkeypatch.setattr(pip.utils, 'get_installed_distributions',
                        partial(mockGetInstalledDistributions,
                                packages=packages))
    imported, found = pluginManager.importPlugins()
    assert imported == 3
    yield pluginManager
    monkeypatch.undo()
    pluginManager.importPlugins()


@pytest.fixture
def testNode(pluginManager, tdir, tconf, node_config_helper_class):
    name = randomText(20)
    nodeReg = genNodeReg(names=[name])
    ha, cliname, cliha = nodeReg[name]
    config_helper = node_config_helper_class(name, tconf, chroot=tdir)
    node = TestNode(name=name, ha=ha, cliname=cliname, cliha=cliha,
                    nodeRegistry=copy(nodeReg),
                    config_helper=config_helper,
                    config=tconf,
                    primaryDecider=None, pluginPaths=None, seed=randomSeed())
    node.start(None)
    yield node
    node.stop()


@pytest.fixture()
def set_info_log_level(request):
    Logger.setLogLevel(logging.INFO)

    def reset():
        Logger.setLogLevel(logging.NOTSET)

    request.addfinalizer(reset)


# ####### SDK


@pytest.fixture(scope='module')
def sdk_pool_name():
    p_name = "pool_name_" + randomText(13)
    yield p_name
    p_dir = os.path.join(os.path.expanduser("~/.indy_client/pool"), p_name)
    if os.path.isdir(p_dir):
        shutil.rmtree(p_dir, ignore_errors=True)


@pytest.fixture(scope='module')
def sdk_wallet_name():
    w_name = "wallet_name_" + randomText(13)
    yield w_name
    w_dir = os.path.join(os.path.expanduser("~/.indy_client/wallet"), w_name)
    if os.path.isdir(w_dir):
        shutil.rmtree(w_dir, ignore_errors=True)


async def _gen_pool_handler(work_dir, name):
    txn_file_name = os.path.join(work_dir, "pool_transactions_genesis")
    pool_config = json.dumps({"genesis_txn": str(txn_file_name)})
    await create_pool_ledger_config(name, pool_config)
    pool_handle = await open_pool_ledger(name, None)
    return pool_handle


@pytest.fixture(scope='module')
def sdk_pool_handle(looper, txnPoolNodeSet, tdirWithPoolTxns, sdk_pool_name):
    pool_handle = looper.loop.run_until_complete(
        _gen_pool_handler(tdirWithPoolTxns, sdk_pool_name))
    yield pool_handle
    looper.loop.run_until_complete(close_pool_ledger(pool_handle))


async def _gen_wallet_handler(pool_name, wallet_name):
    await create_wallet(pool_name, wallet_name, None, None, None)
    wallet_handle = await open_wallet(wallet_name, None, None)
    return wallet_handle


@pytest.fixture(scope='module')
def sdk_wallet_handle(looper, sdk_pool_name, sdk_wallet_name):
    wallet_handle = looper.loop.run_until_complete(
        _gen_wallet_handler(sdk_pool_name, sdk_wallet_name))
    yield wallet_handle
    looper.loop.run_until_complete(close_wallet(wallet_handle))


@pytest.fixture(scope='module')
def sdk_trustee_seed(trustee_data):
    _, seed = trustee_data[0]
    return seed


@pytest.fixture(scope='module')
def sdk_steward_seed(poolTxnStewardData):
    _, seed = poolTxnStewardData
    return seed.decode()


@pytest.fixture(scope='module')
def sdk_client_seed(poolTxnClientData):
    _, seed = poolTxnClientData
    return seed.decode()


@pytest.fixture(scope='module')
def sdk_client_seed2(poolTxnClientNames, poolTxnData):
    name = poolTxnClientNames[1]
    seed = poolTxnData["seeds"][name]
    return seed


@pytest.fixture(scope='module')
def sdk_new_client_seed():
    return "Client10000000000000000000000000"


@pytest.fixture(scope='module')
def sdk_wallet_trustee(looper, sdk_wallet_handle, sdk_trustee_seed):
    (trustee_did, trustee_verkey) = looper.loop.run_until_complete(
        create_and_store_my_did(sdk_wallet_handle,
                                json.dumps({'seed': sdk_trustee_seed})))
    return sdk_wallet_handle, trustee_did


@pytest.fixture(scope='module')
def sdk_wallet_steward(looper, sdk_wallet_handle, sdk_steward_seed):
    (steward_did, steward_verkey) = looper.loop.run_until_complete(
        create_and_store_my_did(sdk_wallet_handle,
                                json.dumps({'seed': sdk_steward_seed})))
    return sdk_wallet_handle, steward_did


@pytest.fixture(scope='module')
def sdk_wallet_client(looper, sdk_wallet_handle, sdk_client_seed):
    (client_did, _) = looper.loop.run_until_complete(
        create_and_store_my_did(sdk_wallet_handle,
                                json.dumps({'seed': sdk_client_seed})))
    return sdk_wallet_handle, client_did


@pytest.fixture(scope='module')
def sdk_wallet_client2(looper, sdk_wallet_handle, sdk_client_seed2):
    (client_did, _) = looper.loop.run_until_complete(
        create_and_store_my_did(sdk_wallet_handle,
                                json.dumps({'seed': sdk_client_seed2})))
    return sdk_wallet_handle, client_did


@pytest.fixture(scope='module')
def sdk_wallet_new_client(looper, sdk_pool_handle, sdk_wallet_steward,
                          sdk_new_client_seed):
    wh, client_did = sdk_add_new_nym(looper, sdk_pool_handle,
                                     sdk_wallet_steward,
                                     seed=sdk_new_client_seed)
    return wh, client_did


@pytest.fixture(scope='module')
def sdk_wallet_new_steward(looper, sdk_pool_handle, sdk_wallet_steward):
    wh, client_did = sdk_add_new_nym(looper, sdk_pool_handle,
                                     sdk_wallet_steward,
                                     alias='new_steward_qwerty',
                                     role='STEWARD')
    return wh, client_did
