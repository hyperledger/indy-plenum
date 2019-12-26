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
from functools import partial
import time
from typing import Dict, Any, List

from indy.pool import create_pool_ledger_config, open_pool_ledger, close_pool_ledger
from indy.wallet import create_wallet, open_wallet, close_wallet
from indy.did import create_and_store_my_did

from ledger.genesis_txn.genesis_txn_file_util import create_genesis_txn_init_ledger
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.common.member.member import Member
from plenum.common.member.steward import Steward
from plenum.common.signer_did import DidSigner
from plenum.common.signer_simple import SimpleSigner
from plenum.test import waits

import gc
import pip
import pytest
import plenum.config as plenum_config
import plenum.server.general_config.ubuntu_platform_config as platform_config
from plenum.common.keygen_utils import initNodeKeysForBothStacks
from plenum.test.greek import genNodeNames
from plenum.test.grouped_load_scheduling import GroupedLoadScheduling
from plenum.test.node_catchup.helper import waitNodeDataEquality, check_last_3pc_master
from plenum.test.pool_transactions.helper import sdk_add_new_nym, sdk_pool_refresh, sdk_add_new_steward_and_node
from plenum.test.simulation.sim_random import DefaultSimRandom
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.view_change.helper import ensure_view_change
from stp_core.common.logging.handlers import TestingHandler
from stp_core.network.port_dispenser import genHa
from stp_core.types import HA
from _pytest.recwarn import WarningsRecorder

from plenum.common.config_util import getConfig
from stp_core.loop.eventually import eventually
from plenum.common.exceptions import BlowUp
from stp_core.common.log import getlogger, Logger
from stp_core.loop.looper import Looper, Prodable
from plenum.common.constants import DATA, NODE, ALIAS, CLIENT_PORT, \
    CLIENT_IP, NYM, CLIENT_STACK_SUFFIX, PLUGIN_BASE_DIR_PATH, ROLE, \
    STEWARD, VALIDATOR, BLS_KEY, TRUSTEE, BLS_KEY_PROOF
from plenum.common.txn_util import getTxnOrderedFields, get_payload_data, get_type
from plenum.common.types import PLUGIN_TYPE_STATS_CONSUMER, f
from plenum.common.util import getNoInstances, randomString
from plenum.server.notifier_plugin_manager import PluginManager
from plenum.test.helper import checkLastClientReqForNode, \
    waitForViewChange, requestReturnedToNode, randomText, \
    mockGetInstalledDistributions, mockImportModule, chk_all_funcs, \
    create_new_test_node, sdk_json_to_request_object, sdk_send_random_requests, \
    sdk_get_and_check_replies, sdk_set_protocol_version, sdk_send_random_and_check, MockTimer, create_pool_txn_data
from plenum.test.node_request.node_request_helper import checkPrePrepared, \
    checkPropagated, checkPrepared, checkCommitted
from plenum.test.plugin.helper import getPluginPath
from plenum.test.test_node import TestNode, Pool, \
    checkNodesConnected, ensureElectionsDone, genNodeReg, getPrimaryReplica, \
    getNonPrimaryReplicas, TestNodeBootstrap
from plenum.common.config_helper import PConfigHelper, PNodeConfigHelper

Logger.setLogLevel(logging.INFO)
logging.getLogger("indy").setLevel(logging.WARNING)
logger = getlogger()

GENERAL_CONFIG_DIR = 'etc/indy'

DEV_NULL_PATH = '/dev/null'
ROCKSDB_WRITE_BUFFER_SIZE = 256 * 1024


def get_data_for_role(pool_txn_data, role):
    name_and_seeds = []
    for txn in pool_txn_data['txns']:
        txn_data = get_payload_data(txn)
        if txn_data.get(ROLE) == role:
            name = txn_data[ALIAS]
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
        # TODO: Default reports are spamming logs when lots of tests are running
        # logger.info("no {} found in the module, using the default: {}".
        #             format(name, value))
    return value


basePath = os.path.dirname(os.path.abspath(__file__))
testPluginBaseDirPath = os.path.join(basePath, "plugin")


overriddenConfigValues = {
    "DefaultPluginPath": {
        PLUGIN_BASE_DIR_PATH: testPluginBaseDirPath,
        PLUGIN_TYPE_STATS_CONSUMER: "stats_consumer"
    },
    "NEW_VIEW_TIMEOUT": 10,
    "INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT": 60,
    "ToleratePrimaryDisconnection": 2,
    "UPDATE_STATE_FRESHNESS": True,
    "Max3PCBatchWait": 0.5
}


@pytest.fixture(scope="module")
def allPluginsPath():
    return [getPluginPath('stats_consumer')]


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
                     'time not acceptable',
                     'IndyError'
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
            msg = str(record.message if record.message else record.msg)
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
                raise BlowUp("{}: {} ".format(record.levelname, msg))

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

    # Reduce memory amplification during running tests in case of RocksDB used
    config.rocksdb_default_config['write_buffer_size'] = ROCKSDB_WRITE_BUFFER_SIZE
    config.rocksdb_default_config['db_log_dir'] = DEV_NULL_PATH

    # FIXME: much more clear solution is to check which key-value storage type is
    # used for each storage and set corresponding config, but for now only RocksDB
    # tuning is supported (now other storage implementations ignore this parameter)
    # so here we set RocksDB configs unconditionally for simplicity.
    config.db_merkle_leaves_config = config.rocksdb_default_config.copy()
    config.db_merkle_nodes_config = config.rocksdb_default_config.copy()
    config.db_state_config = config.rocksdb_default_config.copy()
    config.db_transactions_config = config.rocksdb_default_config.copy()
    config.db_seq_no_db_config = config.rocksdb_default_config.copy()
    config.db_node_status_db_config = config.rocksdb_default_config.copy()
    config.db_state_signature_config = config.rocksdb_default_config.copy()
    config.db_state_ts_db_config = config.rocksdb_default_config.copy()

    return config


@pytest.fixture(scope="module")
def tconf(general_conf_tdir):
    conf = _tconf(general_conf_tdir)
    return conf


@pytest.fixture()
def tconf_for_func(general_conf_tdir_for_func):
    return _tconf(general_conf_tdir_for_func)


@pytest.fixture(scope="module")
def nodeReg(request) -> Dict[str, HA]:
    nodeCount = getValueFromModule(request, "nodeCount", 4)
    return genNodeReg(count=nodeCount)


@pytest.yield_fixture(scope="module")
def looper(txnPoolNodesLooper):
    yield txnPoolNodesLooper


@pytest.fixture(scope="function")
def pool(tdir_for_func, tconf_for_func):
    return Pool(tmpdir=tdir_for_func, config=tconf_for_func)


# noinspection PyIncorrectDocstring
@pytest.fixture(scope="module")
def ensureView(txnPoolNodeSet, looper):
    """
    Ensure that all the nodes in the txnPoolNodeSet are in the same view.
    """
    return waitForViewChange(looper, txnPoolNodeSet)


@pytest.fixture("module")
def delayed_perf_chk(txnPoolNodeSet):
    d = 20
    for node in txnPoolNodeSet:
        node.delayCheckPerformance(d)
    return d


@pytest.fixture(scope="module")
def sent1(looper, sdk_pool_handle,
          sdk_wallet_client):
    request_couple_json = sdk_send_random_requests(
        looper, sdk_pool_handle, sdk_wallet_client, 1)
    return request_couple_json


@pytest.fixture(scope="module")
def reqAcked1(looper, txnPoolNodeSet, sent1, faultyNodes):
    numerOfNodes = len(txnPoolNodeSet)

    request = sdk_json_to_request_object(sent1[0][0])

    # Wait until request received by all nodes
    propTimeout = waits.expectedClientToPoolRequestDeliveryTime(numerOfNodes)
    coros = [partial(checkLastClientReqForNode, node, request)
             for node in txnPoolNodeSet]
    chk_all_funcs(looper, coros, acceptable_fails=faultyNodes,
                  timeout=propTimeout)
    return request


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
                txnPoolNodeSet,
                reqAcked1,
                faultyNodes):
    checkPropagated(looper, txnPoolNodeSet, reqAcked1, faultyNodes)
    return reqAcked1


@pytest.fixture(scope="module")
def preprepared1(looper, txnPoolNodeSet, propagated1, faultyNodes):
    checkPrePrepared(looper,
                     txnPoolNodeSet,
                     propagated1,
                     range(getNoInstances(len(txnPoolNodeSet))),
                     faultyNodes)
    return propagated1


@pytest.fixture(scope="module")
def prepared1(looper, txnPoolNodeSet, preprepared1, faultyNodes):
    checkPrepared(looper,
                  txnPoolNodeSet,
                  preprepared1,
                  range(getNoInstances(len(txnPoolNodeSet))),
                  faultyNodes)
    return preprepared1


@pytest.fixture(scope="module")
def committed1(looper, txnPoolNodeSet, prepared1, faultyNodes):
    checkCommitted(looper,
                   txnPoolNodeSet,
                   prepared1,
                   range(getNoInstances(len(txnPoolNodeSet))),
                   faultyNodes)
    return prepared1


@pytest.fixture(scope="module")
def replied1(looper, txnPoolNodeSet, sdk_wallet_client,
             committed1, faultyNodes, sent1):
    numOfNodes = len(txnPoolNodeSet)
    numOfInstances = getNoInstances(numOfNodes)
    quorum = numOfInstances * (numOfNodes - faultyNodes)

    _, did = sdk_wallet_client

    def checkOrderedCount():
        resp = [requestReturnedToNode(node,
                                      committed1.digest,
                                      instId)
                for node in txnPoolNodeSet for instId in range(numOfInstances)]
        assert resp.count(True) >= quorum

    orderingTimeout = waits.expectedOrderingTime(numOfInstances)
    looper.run(eventually(checkOrderedCount,
                          retryWait=1,
                          timeout=orderingTimeout))

    sdk_get_and_check_replies(looper, sent1)
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
    node_count = getValueFromModule(request, "nodeCount", 4)
    nodes_with_bls = getValueFromModule(request, "nodes_wth_bls", node_count)
    node_names = genNodeNames(node_count)
    return create_pool_txn_data(node_names=node_names,
                                crypto_factory=create_default_bls_crypto_factory(),
                                get_free_port=lambda: genHa()[1],
                                nodes_with_bls=nodes_with_bls)


@pytest.fixture(scope="module")
def tdirWithPoolTxns(config_helper_class, poolTxnData, tdir, tconf):
    import getpass
    logging.debug("current user when creating new pool txn file: {}".
                  format(getpass.getuser()))

    config_helper = config_helper_class(tconf, chroot=tdir)
    ledger = create_genesis_txn_init_ledger(config_helper.genesis_dir, tconf.poolTransactionsFile)

    for item in poolTxnData["txns"]:
        if get_type(item) == NODE:
            ledger.add(item)
    ledger.stop()
    return config_helper.genesis_dir


@pytest.fixture(scope="module")
def domainTxnOrderedFields():
    return getTxnOrderedFields()


@pytest.fixture(scope="module")
def tdirWithDomainTxns(config_helper_class, poolTxnData, tdir, tconf, domainTxnOrderedFields):
    config_helper = config_helper_class(tconf, chroot=tdir)
    ledger = create_genesis_txn_init_ledger(config_helper.genesis_dir, tconf.domainTransactionsFile)

    for item in poolTxnData["txns"]:
        if get_type(item) == NYM:
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
def testNodeClass(patchPluginManager):
    return TestNode


@pytest.fixture(scope="module")
def testNodeBootstrapClass():
    return TestNodeBootstrap


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
                   do_post_node_creation,
                   testNodeBootstrapClass):
    with ExitStack() as exitStack:
        nodes = []
        for nm in poolTxnNodeNames:
            node = exitStack.enter_context(create_new_test_node(
                testNodeClass, node_config_helper_class, nm, tconf, tdir,
                allPluginsPath, bootstrap_cls=testNodeBootstrapClass))
            do_post_node_creation(node)
            txnPoolNodesLooper.add(node)
            nodes.append(node)
        txnPoolNodesLooper.run(checkNodesConnected(nodes))
        ensureElectionsDone(looper=txnPoolNodesLooper, nodes=nodes)
        yield nodes


@pytest.fixture(scope="function")
def txnPoolNodeSetNotStarted(node_config_helper_class,
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
            nodes.append(node)
        yield nodes


@pytest.fixture(scope="module")
def txnPoolCliNodeReg(poolTxnData):
    cliNodeReg = {}
    for txn in poolTxnData["txns"]:
        if get_type(txn) == NODE:
            data = get_payload_data(txn)[DATA]
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
def testNode(pluginManager,
             node_config_helper_class,
             txnPoolNodesLooper,
             tdirWithPoolTxns,
             tdirWithDomainTxns,
             tdir,
             tconf,
             allPluginsPath,
             tdirWithNodeKeepInited,
             do_post_node_creation,
             poolTxnNodeNames):
    node = None
    for p in txnPoolNodesLooper.prodables:
        if 'Alpha' == p.name:
            node = p
    if not node:
        node = create_new_test_node(
            TestNode, node_config_helper_class, 'Alpha', tconf, tdir,
            None)
        do_post_node_creation(node)
        txnPoolNodesLooper.add(node)
        txnPoolNodesLooper.run(checkNodesConnected([node]))
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
def sdk_pool_data(txnPoolNodeSet):
    p_name = "pool_name_" + randomText(13)
    cfg = {"timeout": 20, "extended_timeout": 60, "conn_limit": 100000, "conn_active_timeout": 1000,
           "preordered_nodes": [n.name for n in txnPoolNodeSet]}
    yield p_name, json.dumps(cfg)
    p_dir = os.path.join(os.path.expanduser("~/.indy_client/pool"), p_name)
    if os.path.isdir(p_dir):
        shutil.rmtree(p_dir, ignore_errors=True)


@pytest.fixture(scope='module')
def sdk_wallet_data():
    w_name = "wallet_name_" + randomText(13)
    sdk_wallet_credentials = '{"key": "key"}'
    sdk_wallet_config = json.dumps({"id": w_name})
    yield sdk_wallet_config, sdk_wallet_credentials
    w_dir = os.path.join(os.path.expanduser("~/.indy_client/wallet"), w_name)
    if os.path.isdir(w_dir):
        shutil.rmtree(w_dir, ignore_errors=True)


async def _gen_pool_handler(work_dir, name, open_config):
    txn_file_name = os.path.join(work_dir, "pool_transactions_genesis")
    pool_config = json.dumps({"genesis_txn": str(txn_file_name)})
    await create_pool_ledger_config(name, pool_config)
    pool_handle = await open_pool_ledger(name, open_config)
    return pool_handle


@pytest.fixture(scope='module')
def sdk_pool_handle(looper, txnPoolNodeSet, tdirWithPoolTxns, sdk_pool_data):
    # TODO think about moving protocol version setting to separate
    # fixture like 'sdk_init' since some sdk request builders don't
    # requires pool handle but use protocol version
    sdk_set_protocol_version(looper)
    pool_name, open_config = sdk_pool_data
    pool_handle = looper.loop.run_until_complete(
        _gen_pool_handler(tdirWithPoolTxns, pool_name, open_config))
    yield pool_handle
    try:
        looper.loop.run_until_complete(close_pool_ledger(pool_handle))
    except Exception as e:
        logger.debug("Unhandled exception: {}".format(e))


async def _gen_wallet_handler(wallet_data):
    wallet_config, wallet_credentials = wallet_data
    await create_wallet(wallet_config, wallet_credentials)
    wallet_handle = await open_wallet(wallet_config, wallet_credentials)
    return wallet_handle


@pytest.fixture(scope='module')
def sdk_wallet_handle(looper, sdk_wallet_data):
    wallet_handle = looper.loop.run_until_complete(_gen_wallet_handler(sdk_wallet_data))
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
def sdk_wallet_new_steward(looper, sdk_pool_handle, sdk_wallet_steward):
    wh, client_did = sdk_add_new_nym(looper, sdk_pool_handle,
                                     sdk_wallet_steward,
                                     alias='new_steward_qwerty',
                                     role='STEWARD')
    return wh, client_did


@pytest.fixture(scope='module')
def sdk_wallet_stewards(looper, sdk_wallet_handle, poolTxnStewardNames, poolTxnData):
    stewards = []
    for name in poolTxnStewardNames:
        seed = poolTxnData["seeds"][name]
        (steward_did, steward_verkey) = looper.loop.run_until_complete(
            create_and_store_my_did(sdk_wallet_handle,
                                    json.dumps({'seed': seed})))
        stewards.append((sdk_wallet_handle, steward_did))

    yield stewards


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


@pytest.fixture(scope="module")
def create_node_and_not_start(testNodeClass,
                              node_config_helper_class,
                              tconf,
                              tdir,
                              allPluginsPath,
                              looper,
                              tdirWithPoolTxns,
                              tdirWithDomainTxns,
                              tdirWithNodeKeepInited):
    with ExitStack() as exitStack:
        node = exitStack.enter_context(create_new_test_node(testNodeClass,
                                node_config_helper_class,
                                "Alpha",
                                tconf,
                                tdir,
                                allPluginsPath))
        node.write_manager.on_catchup_finished()
        yield node
        node.stop()


@pytest.fixture(scope='function')
def view_change_done(looper, txnPoolNodeSet):
    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)


@pytest.fixture(scope='function',
                params=['primary', 'non-primary'])
def one_replica_and_others_in_backup_instance(
        request, txnPoolNodeSet, view_change_done):

    # NOTICE: This parametrized fixture triggers view change as pre-condition

    backup_inst_id = 1

    primary = getPrimaryReplica(txnPoolNodeSet, backup_inst_id)
    non_primaries = getNonPrimaryReplicas(txnPoolNodeSet, backup_inst_id)

    if request.param == 'primary':
        return primary, non_primaries
    else:
        return non_primaries[0], [primary] + non_primaries[1:]


@pytest.fixture(scope='function')
def test_node(tdirWithPoolTxns,
              tdirWithDomainTxns,
              poolTxnNodeNames,
              tdirWithNodeKeepInited,
              tdir,
              tconf,
              allPluginsPath):
    node_name = poolTxnNodeNames[0]
    config_helper = PNodeConfigHelper(node_name, tconf, chroot=tdir)
    node = TestNode(
        node_name,
        config_helper=config_helper,
        config=tconf,
        pluginPaths=allPluginsPath)
    yield node
    node.onStopping()  # TODO stop won't call onStopping as we are in Stopped state


@pytest.yield_fixture("module")
def sdk_node_created_after_some_txns(looper, testNodeClass, do_post_node_creation,
                                     sdk_pool_handle, sdk_wallet_client, sdk_wallet_steward,
                                     txnPoolNodeSet, tdir, tconf, allPluginsPath, request):
    txnCount = getValueFromModule(request, "txnCount", 5)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              txnCount)
    new_steward_name = randomString()
    new_node_name = "Epsilon"
    new_steward_wallet_handle, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        new_steward_name, new_node_name, tdir, tconf, nodeClass=testNodeClass,
        allPluginsPath=allPluginsPath, autoStart=True,
        do_post_node_creation=do_post_node_creation)
    sdk_pool_refresh(looper, sdk_pool_handle)
    yield looper, new_node, sdk_pool_handle, new_steward_wallet_handle


@pytest.fixture("module")
def sdk_node_set_with_node_added_after_some_txns(
        txnPoolNodeSet, sdk_node_created_after_some_txns):
    looper, new_node, sdk_pool_handle, new_steward_wallet_handle = \
        sdk_node_created_after_some_txns
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    sdk_pool_refresh(looper, sdk_pool_handle)
    return looper, new_node, sdk_pool_handle, new_steward_wallet_handle


@pytest.fixture("module")
def sdk_new_node_caught_up(txnPoolNodeSet,
                           sdk_node_set_with_node_added_after_some_txns):
    looper, new_node, _, _ = sdk_node_set_with_node_added_after_some_txns
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4],
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    check_last_3pc_master(new_node, txnPoolNodeSet[:4])

    # Check if catchup done once
    catchup_done_once = True
    for leecher in new_node.ledgerManager._node_leecher._leechers.values():
        catchup_done_once = catchup_done_once and (leecher.num_txns_caught_up > 0)

    if not catchup_done_once:
        # It might be the case that node has to do catchup again, in that case
        # check the return value of `num_txns_caught_up_in_last_catchup` to be
        # greater than 0

        assert max(
            getAllReturnVals(
                new_node,
                new_node.num_txns_caught_up_in_last_catchup)) > 0

    return new_node


@pytest.fixture(params=range(100))
def random(request):
    return DefaultSimRandom(request.param)


@pytest.fixture(params=[0, 1576800000.0])
def initial_time(request):
    return request.param


@pytest.fixture
def mock_timer(initial_time):
    return MockTimer(initial_time)


def _select_item_except(index, population, exclude: List = []):
    limited_population = [item for item in population if item not in exclude]
    return limited_population[index]


@pytest.fixture(params=[0, 1, -1])
def some_item(request):
    return partial(_select_item_except, request.param)


@pytest.fixture(params=[0, 1, -1])
def other_item(request):
    return partial(_select_item_except, request.param)


@pytest.fixture(params=[0, 1, -1])
def another_item(request):
    return partial(_select_item_except, request.param)
