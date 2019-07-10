from datetime import datetime
import itertools
import os
import random
import string
from _signal import SIGINT
from contextlib import contextmanager
from functools import partial
from itertools import permutations, combinations
from shutil import copyfile
from sys import executable
from time import sleep
from typing import Tuple, Iterable, Dict, Optional, List, Any, Sequence, Union, Callable

import base58
import pytest
from indy.pool import set_protocol_version

from common.serializers.serialization import invalid_index_serializer
from plenum.common.event_bus import ExternalBus
from plenum.common.signer_simple import SimpleSigner
from plenum.common.timer import QueueTimer
from plenum.config import Max3PCBatchWait
from psutil import Popen
import json
import asyncio

from indy.ledger import sign_and_submit_request, sign_request, submit_request, build_node_request, \
    build_pool_config_request, multi_sign_request
from indy.error import ErrorCode, IndyError

from ledger.genesis_txn.genesis_txn_file_util import genesis_txn_file
from plenum.common.constants import DOMAIN_LEDGER_ID, OP_FIELD_NAME, REPLY, REQNACK, REJECT, \
    CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import RequestNackedException, RequestRejectedException, CommonSdkIOException, \
    PoolLedgerTimeoutException
from plenum.common.messages.node_messages import Reply, PrePrepare, Prepare, Commit
from plenum.common.txn_util import get_req_id, get_from
from plenum.common.types import f, OPERATION
from plenum.common.util import getNoInstances, get_utc_epoch
from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.request import Request
from plenum.server.node import Node
from plenum.server.replica import Replica
from plenum.test import waits
from plenum.test.msgs import randomMsg
from plenum.test.spy_helpers import getLastClientReqReceivedForNode, getAllArgs, getAllReturnVals, \
    getAllMsgReceivedForNode
from plenum.test.test_node import TestNode, TestReplica, \
    getPrimaryReplica, getNonPrimaryReplicas, BUY
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventuallyAll, eventually
from stp_core.loop.looper import Looper
from stp_core.network.util import checkPortAvailable

logger = getlogger()


# noinspection PyUnresolvedReferences


def ordinal(n):
    return "%d%s" % (
        n, "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])


def random_string(length: int) -> str:
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


def send_reqs_batches_and_get_suff_replies(
        looper: Looper,
        txnPoolNodeSet,
        sdk_pool_handle,
        sdk_wallet_client,
        num_reqs: int,
        num_batches=1,
        **kwargs):
    # This method assumes that `num_reqs` <= num_batches*MaxbatchSize
    if num_batches == 1:
        return sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, num_reqs)
    else:
        requests = []
        for _ in range(num_batches - 1):
            requests.extend(
                sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                          sdk_wallet_client, num_reqs // num_batches))
        rem = num_reqs % num_batches
        if rem == 0:
            rem = num_reqs // num_batches
        requests.extend(
            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                      sdk_wallet_client, rem))
        return requests


# noinspection PyIncorrectDocstring
def checkResponseCorrectnessFromNodes(receivedMsgs: Iterable, reqId: int,
                                      fValue: int) -> bool:
    """
    the client must get at least :math:`f+1` responses
    """
    msgs = [(msg[f.RESULT.nm][f.REQ_ID.nm], msg[f.RESULT.nm][f.IDENTIFIER.nm])
            for msg in getRepliesFromClientInbox(receivedMsgs, reqId)]
    groupedMsgs = {}
    for tpl in msgs:
        groupedMsgs[tpl] = groupedMsgs.get(tpl, 0) + 1
    assert max(groupedMsgs.values()) >= fValue + 1


def getRepliesFromClientInbox(inbox, reqId) -> list:
    return list({_: msg for msg, _ in inbox if
                 msg[OP_FIELD_NAME] == REPLY and msg[f.RESULT.nm]
                 [f.REQ_ID.nm] == reqId}.values())


def checkLastClientReqForNode(node: TestNode, expectedRequest: Request):
    recvRequest = getLastClientReqReceivedForNode(node)
    assert recvRequest
    assert expectedRequest.as_dict == recvRequest.as_dict


# noinspection PyIncorrectDocstring


def assertLength(collection: Iterable[Any], expectedLength: int):
    assert len(
        collection) == expectedLength, "Observed length was {} but " \
                                       "expected length was {}". \
        format(len(collection), expectedLength)


def assertEquality(observed: Any, expected: Any, details=None):
    assert observed == expected, "Observed value was {} but expected value " \
                                 "was {}, details: {}".format(observed, expected, details)


def randomOperation():
    return {
        "type": BUY,
        "amount": random.randint(10, 100000)
    }


def random_requests(count):
    return [randomOperation() for _ in range(count)]


def random_request_objects(count, protocol_version):
    req_dicts = random_requests(count)
    return [Request(operation=op, protocolVersion=protocol_version) for op in req_dicts]


def buildCompletedTxnFromReply(request, reply: Reply) -> Dict:
    txn = request.operation
    txn.update(reply)
    return txn


async def msgAll(nodes):
    # test sending messages from every node to every other node
    # TODO split send and check so that the messages can be sent concurrently
    for p in permutations(nodes, 2):
        await sendMessageAndCheckDelivery(p[0], p[1])


def sendMessage(sender: Node,
                reciever: Node,
                msg: Optional[Tuple] = None):
    """
    Sends message from one node to another

    :param nodes:
    :param sender: sender
    :param reciever: recepient
    :param msg: optional message - by default random one generated
    :return:
    """

    logger.debug("Sending msg from {} to {}".format(sender.name, reciever.name))
    msg = msg if msg else randomMsg()
    rid = sender.nodestack.getRemote(reciever.name).uid
    sender.nodestack.send(msg, rid)


async def sendMessageAndCheckDelivery(sender: Node,
                                      reciever: Node,
                                      msg: Optional[Tuple] = None,
                                      method=None,
                                      customTimeout=None):
    """
    Sends message from one node to another and checks that it was delivered

    :param sender: sender
    :param reciever: recepient
    :param msg: optional message - by default random one generated
    :param customTimeout:
    :return:
    """

    logger.debug("Sending msg from {} to {}".format(sender.name, reciever.name))
    msg = msg if msg else randomMsg()
    rid = sender.nodestack.getRemote(reciever.name).uid
    sender.nodestack.send(msg, rid)

    timeout = customTimeout or waits.expectedNodeToNodeMessageDeliveryTime()

    await eventually(checkMessageReceived, msg, reciever, method,
                     retryWait=.1,
                     timeout=timeout,
                     ratchetSteps=10)


def sendMessageToAll(nodes,
                     sender: Node,
                     msg: Optional[Tuple] = None):
    """
    Sends message from one node to all others

    :param nodes:
    :param sender: sender
    :param msg: optional message - by default random one generated
    :return:
    """
    for node in nodes:
        if node != sender:
            sendMessage(sender, node, msg)


async def sendMessageAndCheckDeliveryToAll(nodes,
                                           sender: Node,
                                           msg: Optional[Tuple] = None,
                                           method=None,
                                           customTimeout=None):
    """
    Sends message from one node to all other and checks that it was delivered

    :param nodes:
    :param sender: sender
    :param msg: optional message - by default random one generated
    :param customTimeout:
    :return:
    """
    customTimeout = customTimeout or waits.expectedNodeToAllNodesMessageDeliveryTime(
        len(nodes))
    for node in nodes:
        if node != sender:
            await sendMessageAndCheckDelivery(sender, node, msg, method, customTimeout)
            break


def checkMessageReceived(msg, receiver, method: str = None):
    allMsgs = getAllMsgReceivedForNode(receiver, method)
    assert msg in allMsgs


def addNodeBack(node_set,
                looper: Looper,
                node: Node,
                tconf,
                tdir) -> TestNode:
    config_helper = PNodeConfigHelper(node.name, tconf, chroot=tdir)
    restartedNode = TestNode(node.name,
                             config_helper=config_helper,
                             config=tconf,
                             ha=node.nodestack.ha,
                             cliha=node.clientstack.ha)
    for node in node_set:
        if node.name != restartedNode.name:
            node.nodestack.reconnectRemoteWithName(restartedNode.name)
    node_set.append(restartedNode)
    looper.add(restartedNode)
    return restartedNode


def checkPropagateReqCountOfNode(node: TestNode, digest: str):
    assert digest in node.requests
    assert node.quorums.propagate.is_reached(
        len(node.requests[digest].propagates))


def requestReturnedToNode(node: TestNode, key: str,
                          instId: int):
    params = getAllArgs(node, node.processOrdered)
    # Skipping the view no and time from each ordered request
    recvdOrderedReqs = [
        (p['ordered'].instId, p['ordered'].valid_reqIdr[0]) for p in params]
    expected = (instId, key)
    return expected in recvdOrderedReqs


def checkRequestReturnedToNode(node: TestNode, key: str,
                               instId: int):
    assert requestReturnedToNode(node, key, instId)


def checkRequestNotReturnedToNode(node: TestNode, key: str,
                                  instId: int):
    assert not requestReturnedToNode(node, key, instId)


def check_request_is_not_returned_to_nodes(txnPoolNodeSet, request):
    instances = range(getNoInstances(len(txnPoolNodeSet)))
    for node, inst_id in itertools.product(txnPoolNodeSet, instances):
        checkRequestNotReturnedToNode(node,
                                      request.key,
                                      inst_id)


def checkPrePrepareReqSent(replica: TestReplica, req: Request):
    prePreparesSent = getAllArgs(replica, replica.sendPrePrepare)
    expectedDigest = TestReplica.batchDigest([req])
    assert expectedDigest in [p["ppReq"].digest for p in prePreparesSent]
    assert (req.digest,) in \
           [p["ppReq"].reqIdr for p in prePreparesSent]


def checkPrePrepareReqRecvd(replicas: Iterable[TestReplica],
                            expectedRequest: PrePrepare):
    for replica in replicas:
        params = getAllArgs(replica, replica._can_process_pre_prepare)
        assert expectedRequest.reqIdr in [p['pre_prepare'].reqIdr for p in params]


def checkPrepareReqSent(replica: TestReplica, key: str,
                        view_no: int):
    paramsList = getAllArgs(replica, replica.canPrepare)
    rv = getAllReturnVals(replica,
                          replica.canPrepare)
    args = [p["ppReq"].reqIdr for p in paramsList if p["ppReq"].viewNo == view_no]
    assert (key,) in args
    idx = args.index((key,))
    assert rv[idx]


def checkSufficientPrepareReqRecvd(replica: TestReplica, viewNo: int,
                                   ppSeqNo: int):
    key = (viewNo, ppSeqNo)
    assert key in replica.prepares
    assert len(replica.prepares[key][1]) >= replica.quorums.prepare.value


def checkSufficientCommitReqRecvd(replicas: Iterable[TestReplica], viewNo: int,
                                  ppSeqNo: int):
    for replica in replicas:
        key = (viewNo, ppSeqNo)
        assert key in replica.commits
        received = len(replica.commits[key][1])
        minimum = replica.quorums.commit.value
        assert received > minimum


def checkViewNoForNodes(nodes: Iterable[TestNode], expectedViewNo: int = None):
    """
    Checks if all the given nodes have the expected view no

    :param nodes: The nodes to check for
    :param expectedViewNo: the view no that the nodes are expected to have
    :return:
    """

    viewNos = set()
    for node in nodes:
        logger.debug("{}'s view no is {}".format(node, node.viewNo))
        viewNos.add(node.viewNo)
    assert len(viewNos) == 1, 'Expected 1, but got {}. ' \
                              'ViewNos: {}'.format(len(viewNos), [(n.name, n.viewNo) for n in nodes])
    vNo, = viewNos
    if expectedViewNo is not None:
        assert vNo >= expectedViewNo, \
            'Expected at least {}, but got {}'.format(expectedViewNo, vNo)
    return vNo


def waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=None,
                      customTimeout=None):
    """
    Waits for nodes to come to same view.
    Raises exception when time is out
    """

    timeout = customTimeout or waits.expectedPoolElectionTimeout(len(txnPoolNodeSet))
    return looper.run(eventually(checkViewNoForNodes,
                                 txnPoolNodeSet,
                                 expectedViewNo,
                                 timeout=timeout))


def getNodeSuspicions(node: TestNode, code: int = None):
    params = getAllArgs(node, TestNode.reportSuspiciousNode)
    if params and code is not None:
        params = [param for param in params
                  if 'code' in param and param['code'] == code]
    return params


def checkDiscardMsg(processors, discardedMsg,
                    reasonRegexp, *exclude):
    if not exclude:
        exclude = []
    for p in filterNodeSet(processors, exclude):
        last = p.spylog.getLastParams(p.discard, required=False)
        assert last
        assert last['msg'] == discardedMsg
        assert reasonRegexp in last['reason']


def countDiscarded(processor, reasonPat):
    c = 0
    for entry in processor.spylog.getAll(processor.discard):
        if 'reason' in entry.params and (
                (isinstance(
                    entry.params['reason'],
                    str) and reasonPat in entry.params['reason']),
                (reasonPat in str(
                    entry.params['reason']))):
            c += 1
    return c


def filterNodeSet(nodeSet, exclude: List[Union[str, Node]]):
    """
    Return a set of nodes with the nodes in exclude removed.

    :param nodeSet: the set of nodes
    :param exclude: the list of nodes or node names to exclude
    :return: the filtered nodeSet
    """
    return [n for n in nodeSet
            if n not in
            [nodeSet[x] if isinstance(x, str) else x for x in exclude]]


def whitelistNode(toWhitelist: str, frm: Sequence[TestNode], *codes):
    for node in frm:
        node.whitelistNode(toWhitelist, *codes)


def whitelistClient(toWhitelist: str, frm: Sequence[TestNode], *codes):
    for node in frm:
        node.whitelistClient(toWhitelist, *codes)


def assertExp(condition):
    assert condition


def assert_eq(actual, expected):
    assert actual == expected


def assert_in(value, collection):
    assert value in collection


def assertFunc(func):
    assert func()


def checkLedgerEquality(ledger1, ledger2):
    assertLength(ledger1, ledger2.size)
    assertEquality(ledger1.root_hash, ledger2.root_hash)
    assertEquality(ledger1.uncommitted_root_hash, ledger2.uncommitted_root_hash)


def checkAllLedgersEqual(*ledgers):
    for l1, l2 in combinations(ledgers, 2):
        checkLedgerEquality(l1, l2)


def checkStateEquality(state1, state2):
    if state1 is None:
        return state2 is None
    assertEquality(state1.as_dict, state2.as_dict)
    assertEquality(state1.committedHeadHash, state2.committedHeadHash)
    assertEquality(state1.committedHead, state2.committedHead)


def check_seqno_db_equality(db1, db2):
    assert db1.size == db2.size, \
        "{} != {}".format(db1.size, db2.size)
    assert {bytes(k): bytes(v) for k, v in db1._keyValueStorage.iterator()} == \
           {bytes(k): bytes(v) for k, v in db2._keyValueStorage.iterator()}


def check_primaries_equality(node1, node2):
    assert node1.primaries == node2.primaries, \
        "{} != {}".format(node1.primaries, node2.primaries)


def check_last_ordered_3pc(node1, node2):
    master_replica_1 = node1.master_replica
    master_replica_2 = node2.master_replica
    assert master_replica_1.last_ordered_3pc == master_replica_2.last_ordered_3pc, \
        "{} != {}".format(master_replica_1.last_ordered_3pc,
                          master_replica_2.last_ordered_3pc)
    return master_replica_1.last_ordered_3pc


def check_last_ordered_3pc_backup(node1, node2):
    assert len(node1.replicas) == len(node2.replicas)
    for i in range(1, len(node1.replicas)):
        replica1 = node1.replicas[i]
        replica2 = node2.replicas[i]
        assert replica1.last_ordered_3pc == replica2.last_ordered_3pc, \
            "{}: {} != {}: {}".format(replica1, replica1.last_ordered_3pc,
                                      replica2, replica2.last_ordered_3pc)


def check_view_no(node1, node2):
    assert node1.viewNo == node2.viewNo, \
        "{} != {}".format(node1.viewNo, node2.viewNo)


def check_last_ordered_3pc_on_all_replicas(nodes, last_ordered_3pc):
    for n in nodes:
        for r in n.replicas.values():
            assert r.last_ordered_3pc == last_ordered_3pc, \
                "{} != {}".format(r.last_ordered_3pc,
                                  last_ordered_3pc)


def check_last_ordered_3pc_on_master(nodes, last_ordered_3pc):
    for n in nodes:
        assert n.master_replica.last_ordered_3pc == last_ordered_3pc, \
            "{} != {}".format(n.master_replica.last_ordered_3pc,
                              last_ordered_3pc)


def check_last_ordered_3pc_on_backup(nodes, last_ordered_3pc):
    for n in nodes:
        for i, r in n.replicas.items():
            if i != 0:
                assert r.last_ordered_3pc == last_ordered_3pc, \
                    "{} != {}".format(r.last_ordered_3pc,
                                      last_ordered_3pc)


def randomText(size):
    return ''.join(random.choice(string.ascii_letters) for _ in range(size))


def mockGetInstalledDistributions(packages):
    ret = []
    for pkg in packages:
        obj = type('', (), {})()
        obj.key = pkg
        ret.append(obj)
    return ret


def mockImportModule(moduleName):
    obj = type(moduleName, (), {})()
    obj.send_message = lambda *args: None
    return obj


def initDirWithGenesisTxns(
        dirName,
        tconf,
        tdirWithPoolTxns=None,
        tdirWithDomainTxns=None,
        new_pool_txn_file=None,
        new_domain_txn_file=None):
    os.makedirs(dirName, exist_ok=True)
    if tdirWithPoolTxns:
        new_pool_txn_file = new_pool_txn_file or tconf.poolTransactionsFile
        copyfile(
            os.path.join(
                tdirWithPoolTxns, genesis_txn_file(
                    tconf.poolTransactionsFile)), os.path.join(
                dirName, genesis_txn_file(new_pool_txn_file)))
    if tdirWithDomainTxns:
        new_domain_txn_file = new_domain_txn_file or tconf.domainTransactionsFile
        copyfile(
            os.path.join(
                tdirWithDomainTxns, genesis_txn_file(
                    tconf.domainTransactionsFile)), os.path.join(
                dirName, genesis_txn_file(new_domain_txn_file)))


def stopNodes(nodes: List[TestNode], looper=None, ensurePortsFreedUp=True):
    if ensurePortsFreedUp:
        assert looper, 'Need a looper to make sure ports are freed up'

    for node in nodes:
        node.stop()

    if ensurePortsFreedUp:
        ports = [[n.nodestack.ha[1], n.clientstack.ha[1]] for n in nodes]
        waitUntilPortIsAvailable(looper, ports)


def waitUntilPortIsAvailable(looper, ports, timeout=5):
    ports = itertools.chain(*ports)

    def chk():
        for port in ports:
            checkPortAvailable(("", port))

    looper.run(eventually(chk, retryWait=.5, timeout=timeout))


def run_script(script, *args):
    s = os.path.join(os.path.dirname(__file__), '../../scripts/' + script)
    command = [executable, s]
    command.extend(args)

    with Popen([executable, s]) as p:
        sleep(4)
        p.send_signal(SIGINT)
        p.wait(timeout=1)
        assert p.poll() == 0, 'script failed'


def viewNoForNodes(nodes):
    viewNos = {node.viewNo for node in nodes}
    assert 1 == len(viewNos)
    return next(iter(viewNos))


def primaryNodeNameForInstance(nodes, instanceId):
    primaryNames = {node.replicas[instanceId].primaryName for node in nodes}
    assert 1 == len(primaryNames)
    primaryReplicaName = next(iter(primaryNames))
    return primaryReplicaName[:-2]


def nodeByName(nodes, name):
    for node in nodes:
        if node.name == name:
            return node
    raise Exception("Node with the name '{}' has not been found.".format(name))


def send_pre_prepare(view_no, pp_seq_no, nodes,
                     state_root=None, txn_root=None):
    pre_prepare = PrePrepare(
        0,
        view_no,
        pp_seq_no,
        get_utc_epoch(),
        ["requests digest"],
        0,
        "random digest",
        DOMAIN_LEDGER_ID,
        state_root or '0' * 44,
        txn_root or '0' * 44,
        0,
        True
    )
    primary_node = getPrimaryReplica(nodes).node
    non_primary_nodes = set(nodes) - {primary_node}

    sendMessageToAll(nodes, primary_node, pre_prepare)
    for non_primary_node in non_primary_nodes:
        sendMessageToAll(nodes, non_primary_node, pre_prepare)


def send_prepare(view_no, pp_seq_no, nodes, state_root=None, txn_root=None):
    prepare = Prepare(
        0,
        view_no,
        pp_seq_no,
        get_utc_epoch(),
        "random digest",
        state_root or '0' * 44,
        txn_root or '0' * 44
    )
    primary_node = getPrimaryReplica(nodes).node
    sendMessageToAll(nodes, primary_node, prepare)


def send_commit(view_no, pp_seq_no, nodes):
    commit = Commit(
        0,
        view_no,
        pp_seq_no)
    primary_node = getPrimaryReplica(nodes).node
    sendMessageToAll(nodes, primary_node, commit)


def get_key_from_req(req: dict):
    return Request(identifier=req[f.IDENTIFIER.nm],
                   reqId=req[f.REQ_ID.nm],
                   operation=req[OPERATION],
                   protocolVersion=req[f.PROTOCOL_VERSION.nm],
                   signature=req.get(f.SIG.nm),
                   taaAcceptance=req.get(f.TAA_ACCEPTANCE)
                   ).key


def chk_all_funcs(looper, funcs, acceptable_fails=0, retry_wait=None,
                  timeout=None, override_eventually_timeout=False):
    # TODO: Move this logic to eventuallyAll
    def chk():
        fails = 0
        last_ex = None
        for func in funcs:
            try:
                func()
            except Exception as ex:
                fails += 1
                if fails >= acceptable_fails:
                    logger.debug('Too many fails, the last one: {}'.format(repr(ex)))
                last_ex = ex
        assert fails <= acceptable_fails, '{} out of {} failed. Last exception:' \
                                          ' {}'.format(fails, len(funcs), last_ex)

    kwargs = {}
    if retry_wait:
        kwargs['retryWait'] = retry_wait
    if timeout:
        kwargs['timeout'] = timeout
    if override_eventually_timeout:
        kwargs['override_timeout_limit'] = override_eventually_timeout

    looper.run(eventually(chk, **kwargs))


def check_request_ordered(node, request: Request):
    # it's ok to iterate through all txns since this is a test
    for seq_no, txn in node.domainLedger.getAllTxn():
        if get_req_id(txn) is None:
            continue
        if get_from(txn) is None:
            continue
        if get_req_id(txn) != request.reqId:
            continue
        if get_from(txn) != request.identifier:
            continue
        return True
    raise ValueError('{} request not ordered by node {}'.format(request, node.name))


def wait_for_requests_ordered(looper, nodes, requests):
    node_count = len(nodes)
    timeout_per_request = waits.expectedTransactionExecutionTime(node_count)
    total_timeout = (1 + len(requests) / 10) * timeout_per_request
    coros = [partial(check_request_ordered,
                     node,
                     request)
             for (node, request) in list(itertools.product(nodes, requests))]
    looper.run(eventuallyAll(*coros, retryWait=1, totalTimeout=total_timeout))


def create_new_test_node(test_node_class, node_config_helper_class, name, conf,
                         tdir, plugin_paths, bootstrap_cls=None,
                         node_ha=None, client_ha=None):
    config_helper = node_config_helper_class(name, conf, chroot=tdir)
    return test_node_class(name,
                           config_helper=config_helper,
                           config=conf,
                           pluginPaths=plugin_paths,
                           ha=node_ha,
                           cliha=client_ha,
                           bootstrap_cls=bootstrap_cls)


# ####### SDK


def sdk_gen_request(operation, protocol_version=CURRENT_PROTOCOL_VERSION,
                    identifier=None, **kwargs):
    # Question: Why this method is called sdk_gen_request? It does not use
    # the indy-sdk
    return Request(operation=operation, reqId=random.randint(10, 1000000000),
                   protocolVersion=protocol_version, identifier=identifier,
                   **kwargs)


def sdk_gen_pool_request(looper, sdk_wallet_new_steward, node_alias, node_did):
    _, new_steward_did = sdk_wallet_new_steward

    node_ip = '{}.{}.{}.{}'.format(
        random.randint(1, 240),
        random.randint(1, 240),
        random.randint(1, 240),
        random.randint(1, 240))
    data = {
        'alias': node_alias,
        'client_port': 50001,
        'node_port': 50002,
        'node_ip': node_ip,
        'client_ip': node_ip,
        'services': []
    }

    req = looper.loop.run_until_complete(
        build_node_request(new_steward_did, node_did, json.dumps(data)))

    return Request(**json.loads(req))


def sdk_random_request_objects(count, protocol_version, identifier=None,
                               **kwargs):
    ops = random_requests(count)
    return [sdk_gen_request(op, protocol_version=protocol_version,
                            identifier=identifier, **kwargs) for op in ops]


def sdk_sign_request_objects(looper, sdk_wallet, reqs: Sequence):
    wallet_h, did = sdk_wallet
    reqs_str = [json.dumps(req.as_dict) for req in reqs]
    reqs = [looper.loop.run_until_complete(sign_request(wallet_h, did, req))
            for req in reqs_str]
    return reqs


def sdk_multi_sign_request_objects(looper, sdk_wallets, reqs: Sequence):
    reqs_str = [json.dumps(req.as_dict) for req in reqs]
    for sdk_wallet in sdk_wallets:
        wallet_h, did = sdk_wallet
        reqs_str = [looper.loop.run_until_complete(multi_sign_request(wallet_h, did, req))
                    for req in reqs_str]
    return reqs_str


def sdk_sign_request_strings(looper, sdk_wallet, reqs: Sequence):
    wallet_h, did = sdk_wallet
    reqs_str = [json.dumps(req) for req in reqs]
    reqs = [looper.loop.run_until_complete(sign_request(wallet_h, did, req))
            for req in reqs_str]
    return reqs


def sdk_multisign_request_object(looper, sdk_wallet, req):
    wh, did = sdk_wallet
    return looper.loop.run_until_complete(multi_sign_request(wh, did, req))


def sdk_signed_random_requests(looper, sdk_wallet, count):
    _, did = sdk_wallet
    reqs_obj = sdk_random_request_objects(count, identifier=did,
                                          protocol_version=CURRENT_PROTOCOL_VERSION)
    return sdk_sign_request_objects(looper, sdk_wallet, reqs_obj)


def sdk_send_signed_requests(pool_h, signed_reqs: Sequence):
    return [(json.loads(req),
             asyncio.ensure_future(submit_request(pool_h, req)))
            for req in signed_reqs]


def sdk_send_random_requests(looper, pool_h, sdk_wallet, count: int):
    reqs = sdk_signed_random_requests(looper, sdk_wallet, count)
    return sdk_send_signed_requests(pool_h, reqs)


def sdk_send_random_request(looper, pool_h, sdk_wallet):
    rets = sdk_send_random_requests(looper, pool_h, sdk_wallet, 1)
    return rets[0]


def sdk_send_random_pool_requests(looper, pool_h, sdk_wallet_new_steward, count: int):
    node_alias = random_string(7)
    node_did = SimpleSigner(seed=random_string(32).encode()).identifier

    reqs = [sdk_gen_pool_request(looper, sdk_wallet_new_steward, node_alias, node_did) for _ in range(count)]
    return [sdk_sign_and_submit_req_obj(looper, pool_h, sdk_wallet_new_steward, req) for req in reqs]


def sdk_send_random_pool_and_domain_requests(looper, pool_h, sdk_wallet_new_steward, count: int):
    node_alias = random_string(7)
    node_did = SimpleSigner(seed=random_string(32).encode()).identifier

    req_gens = [
        lambda: sdk_gen_request(random_requests(1)[0], identifier=sdk_wallet_new_steward[1]),
        lambda: sdk_gen_pool_request(looper, sdk_wallet_new_steward, node_alias, node_did),
    ]

    res = []
    for i in range(count):
        req = req_gens[i % len(req_gens)]()
        res.append(sdk_sign_and_submit_req_obj(looper, pool_h, sdk_wallet_new_steward, req))
        looper.runFor(0.1)  # Give nodes some time to start ordering, so that requests are really alternating
    return res


def sdk_sign_and_submit_req(pool_handle, sdk_wallet, req):
    wallet_handle, sender_did = sdk_wallet
    return json.loads(req), asyncio.ensure_future(
        sign_and_submit_request(pool_handle, wallet_handle, sender_did, req))


def sdk_sign_and_submit_req_obj(looper, pool_handle, sdk_wallet, req_obj):
    s_req = sdk_sign_request_objects(looper, sdk_wallet, [req_obj])[0]
    return sdk_send_signed_requests(pool_handle, [s_req])[0]


def sdk_sign_and_submit_op(looper, pool_handle, sdk_wallet, op):
    _, did = sdk_wallet
    req_obj = sdk_gen_request(op, protocol_version=CURRENT_PROTOCOL_VERSION,
                              identifier=did)
    s_req = sdk_sign_request_objects(looper, sdk_wallet, [req_obj])[0]
    return sdk_send_signed_requests(pool_handle, [s_req])[0]


def sdk_get_reply(looper, sdk_req_resp, timeout=None):
    req_json, resp_task = sdk_req_resp
    # TODO: change timeout evaluating logic, when sdk will can tuning timeout from outside
    if timeout is None:
        timeout = waits.expectedTransactionExecutionTime(7)
    try:
        resp = looper.run(asyncio.wait_for(resp_task, timeout=timeout))
        resp = json.loads(resp)
    except IndyError as e:
        resp = e.error_code
    except TimeoutError as e:
        resp = ErrorCode.PoolLedgerTimeout

    return req_json, resp


# TODO: Check places where sdk_get_replies used without sdk_check_reply
# We need to be sure that test behaviour don't need to check response
# validity
def sdk_get_replies(looper, sdk_req_resp: Sequence, timeout=None):
    resp_tasks = [resp for _, resp in sdk_req_resp]
    # TODO: change timeout evaluating logic, when sdk will can tuning timeout from outside
    if timeout is None:
        timeout = waits.expectedTransactionExecutionTime(7)

    def get_res(task, done_list):
        if task in done_list:
            try:
                resp = json.loads(task.result())
            except IndyError as e:
                resp = e.error_code
        else:
            resp = ErrorCode.PoolLedgerTimeout
        return resp

    done, pending = looper.run(asyncio.wait(resp_tasks, timeout=timeout))
    if pending:
        for task in pending:
            task.cancel()
    ret = [(req, get_res(resp, done)) for req, resp in sdk_req_resp]
    return ret


def sdk_check_reply(req_res):
    req, res = req_res
    if isinstance(res, ErrorCode):
        if res == ErrorCode.PoolLedgerTimeout:
            raise PoolLedgerTimeoutException('Got PoolLedgerTimeout for request {}'
                                             .format(req))
        else:
            raise CommonSdkIOException('Got an error with code {} for request {}'
                                       .format(res, req))
    if not isinstance(res, dict):
        raise CommonSdkIOException("Unexpected response format {}".format(res))

    def _parse_op(res_dict):
        if res_dict['op'] == REQNACK:
            raise RequestNackedException('ReqNack of id {}. Reason: {}'
                                         .format(req['reqId'], res_dict['reason']))
        if res_dict['op'] == REJECT:
            raise RequestRejectedException('Reject of id {}. Reason: {}'
                                           .format(req['reqId'], res_dict['reason']))

    if 'op' in res:
        _parse_op(res)
    else:
        for resps in res.values():
            if isinstance(resps, str):
                _parse_op(json.loads(resps))
            elif isinstance(resps, dict):
                _parse_op(resps)
            else:
                raise CommonSdkIOException("Unexpected response format {}".format(res))


def sdk_get_and_check_replies(looper, sdk_req_resp: Sequence, timeout=None):
    rets = []
    for req_res in sdk_get_replies(looper, sdk_req_resp, timeout):
        sdk_check_reply(req_res)
        rets.append(req_res)
    return rets


def sdk_eval_timeout(req_count: int, node_count: int,
                     customTimeoutPerReq: float = None, add_delay_to_timeout: float = 0):
    timeout_per_request = customTimeoutPerReq or waits.expectedTransactionExecutionTime(node_count)
    timeout_per_request += add_delay_to_timeout
    # here we try to take into account what timeout for execution
    # N request - total_timeout should be in
    # timeout_per_request < total_timeout < timeout_per_request * N
    # we cannot just take (timeout_per_request * N) because it is so huge.
    # (for timeout_per_request=5 and N=10, total_timeout=50sec)
    # lets start with some simple formula:
    return (1 + req_count / 10) * timeout_per_request


def sdk_send_and_check(signed_reqs, looper, txnPoolNodeSet, pool_h, timeout=None):
    if not timeout:
        timeout = sdk_eval_timeout(len(signed_reqs), len(txnPoolNodeSet))
    results = sdk_send_signed_requests(pool_h, signed_reqs)
    sdk_replies = sdk_get_replies(looper, results, timeout=timeout)
    for req_res in sdk_replies:
        sdk_check_reply(req_res)
    return sdk_replies


def sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool, sdk_wallet, count,
                              customTimeoutPerReq: float = None, add_delay_to_timeout: float = 0,
                              override_timeout_limit=False, total_timeout=None):
    sdk_reqs = sdk_send_random_requests(looper, sdk_pool, sdk_wallet, count)
    if not total_timeout:
        total_timeout = sdk_eval_timeout(len(sdk_reqs), len(txnPoolNodeSet),
                                         customTimeoutPerReq=customTimeoutPerReq,
                                         add_delay_to_timeout=add_delay_to_timeout)
    sdk_replies = sdk_get_replies(looper, sdk_reqs, timeout=total_timeout)
    for req_res in sdk_replies:
        sdk_check_reply(req_res)
    return sdk_replies


def sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool, sdk_wallet,
                                         num_reqs, num_batches=1, **kwargs):
    # This method assumes that `num_reqs` <= num_batches*MaxbatchSize
    if num_reqs < num_batches:
        raise BaseException(
            'sdk_send_batches_of_random_and_check method assumes that `num_reqs` <= num_batches*MaxbatchSize')
    if num_batches == 1:
        return sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool, sdk_wallet, num_reqs, **kwargs)

    reqs_in_batch = num_reqs // num_batches
    reqs_in_last_batch = reqs_in_batch + num_reqs % num_batches

    sdk_replies = []
    for _ in range(num_batches - 1):
        sdk_replies.extend(sdk_send_random_and_check(looper, txnPoolNodeSet,
                                                     sdk_pool, sdk_wallet,
                                                     reqs_in_batch, **kwargs))
    sdk_replies.extend(sdk_send_random_and_check(looper, txnPoolNodeSet,
                                                 sdk_pool, sdk_wallet,
                                                 reqs_in_last_batch, **kwargs))
    return sdk_replies


def sdk_send_batches_of_random(looper, txnPoolNodeSet, sdk_pool, sdk_wallet,
                               num_reqs, num_batches=1, timeout=Max3PCBatchWait):
    if num_reqs < num_batches:
        raise BaseException(
            'sdk_send_batches_of_random_and_check method assumes that `num_reqs` <= num_batches*MaxbatchSize')
    if num_batches == 1:
        sdk_reqs = sdk_send_random_requests(looper, sdk_pool, sdk_wallet, num_reqs)
        looper.runFor(timeout)
        return sdk_reqs

    reqs_in_batch = num_reqs // num_batches
    reqs_in_last_batch = reqs_in_batch + num_reqs % num_batches

    sdk_reqs = []
    for _ in range(num_batches - 1):
        sdk_reqs.extend(sdk_send_random_requests(looper, sdk_pool, sdk_wallet, reqs_in_batch))
        looper.runFor(timeout)
    sdk_reqs.extend(sdk_send_random_requests(looper, sdk_pool, sdk_wallet, reqs_in_last_batch))
    looper.runFor(timeout)
    return sdk_reqs


def sdk_sign_request_from_dict(looper, sdk_wallet, op, reqId=None, taa_acceptance=None):
    wallet_h, did = sdk_wallet
    reqId = reqId or random.randint(10, 100000)
    request = Request(operation=op, reqId=reqId,
                      protocolVersion=CURRENT_PROTOCOL_VERSION, identifier=did,
                      taaAcceptance=taa_acceptance)
    req_str = json.dumps(request.as_dict)
    resp = looper.loop.run_until_complete(sign_request(wallet_h, did, req_str))
    return json.loads(resp)


def sdk_check_request_is_not_returned_to_nodes(looper, nodeSet, request):
    instances = range(getNoInstances(len(nodeSet)))
    coros = []
    for node, inst_id in itertools.product(nodeSet, instances):
        c = partial(checkRequestNotReturnedToNode,
                    node=node,
                    identifier=request['identifier'],
                    reqId=request['reqId'],
                    instId=inst_id
                    )
        coros.append(c)
    timeout = waits.expectedTransactionExecutionTime(len(nodeSet))
    looper.run(eventuallyAll(*coros, retryWait=1, totalTimeout=timeout))


def sdk_json_to_request_object(json_req):
    return Request(identifier=json_req.get('identifier', None),
                   reqId=json_req['reqId'],
                   operation=json_req['operation'],
                   signature=json_req['signature'] if 'signature' in json_req else None,
                   protocolVersion=json_req['protocolVersion'] if 'protocolVersion' in json_req else None,
                   taaAcceptance=json_req.get('taaAcceptance', None))


def sdk_json_couples_to_request_list(json_couples):
    req_list = []
    for json_couple in json_couples:
        req_list.append(sdk_json_to_request_object(json_couple[0]))
    return req_list


def sdk_get_bad_response(looper, reqs, exception, message):
    with pytest.raises(exception) as e:
        sdk_get_and_check_replies(looper, reqs)
    assert message in e._excinfo[1].args[0]


def sdk_set_protocol_version(looper, version=CURRENT_PROTOCOL_VERSION):
    looper.loop.run_until_complete(set_protocol_version(version))


# Context managers to be used with tconf fixture

@contextmanager
def perf_monitor_disabled(tconf):
    old_unsafe = tconf.unsafe.copy()
    tconf.unsafe.add("disable_view_change")
    yield tconf
    tconf.unsafe = old_unsafe


@contextmanager
def view_change_timeout(tconf, vc_timeout, catchup_timeout=None, propose_timeout=None, ic_timeout=None):
    old_catchup_timeout = tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    old_view_change_timeout = tconf.VIEW_CHANGE_TIMEOUT
    old_propose_timeout = tconf.INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT
    old_propagate_request_delay = tconf.PROPAGATE_REQUEST_DELAY
    old_ic_timeout = tconf.INSTANCE_CHANGE_TIMEOUT
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = \
        0.6 * vc_timeout if catchup_timeout is None else catchup_timeout
    tconf.VIEW_CHANGE_TIMEOUT = vc_timeout
    tconf.INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT = vc_timeout if propose_timeout is None else propose_timeout
    tconf.PROPAGATE_REQUEST_DELAY = 0
    if ic_timeout is not None:
        tconf.INSTANCE_CHANGE_TIMEOUT = ic_timeout
    yield tconf
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = old_catchup_timeout
    tconf.VIEW_CHANGE_TIMEOUT = old_view_change_timeout
    tconf.INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT = old_propose_timeout
    tconf.PROPAGATE_REQUEST_DELAY = old_propagate_request_delay
    tconf.INSTANCE_CHANGE_TIMEOUT = old_ic_timeout


@contextmanager
def max_3pc_batch_limits(tconf, size, wait=10000):
    old_size = tconf.Max3PCBatchSize
    old_wait = tconf.Max3PCBatchWait
    tconf.Max3PCBatchSize = size
    tconf.Max3PCBatchWait = wait
    yield tconf
    tconf.Max3PCBatchSize = old_size
    tconf.Max3PCBatchWait = old_wait


@contextmanager
def freshness(tconf, enabled, timeout):
    old_update_state = tconf.UPDATE_STATE_FRESHNESS
    old_timeout = tconf.STATE_FRESHNESS_UPDATE_INTERVAL
    tconf.UPDATE_STATE_FRESHNESS = enabled
    tconf.STATE_FRESHNESS_UPDATE_INTERVAL = timeout
    yield tconf
    tconf.UPDATE_STATE_FRESHNESS = old_update_state
    tconf.STATE_FRESHNESS_UPDATE_INTERVAL = old_timeout


@contextmanager
def primary_disconnection_time(tconf, value):
    old_tolarate_disconnection = tconf.ToleratePrimaryDisconnection
    tconf.ToleratePrimaryDisconnection = value
    yield tconf
    tconf.ToleratePrimaryDisconnection = old_tolarate_disconnection


@contextmanager
def acc_monitor(tconf, acc_monitor_enabled=True, acc_monitor_timeout=3, acc_monitor_delta=0):
    old_timeout = tconf.ACC_MONITOR_TIMEOUT
    old_delta = tconf.ACC_MONITOR_TXN_DELTA_K
    old_acc_monitor_enabled = tconf.ACC_MONITOR_ENABLED

    tconf.ACC_MONITOR_TIMEOUT = acc_monitor_timeout
    tconf.ACC_MONITOR_TXN_DELTA_K = acc_monitor_delta
    tconf.ACC_MONITOR_ENABLED = acc_monitor_enabled
    yield tconf

    tconf.ACC_MONITOR_TIMEOUT = old_timeout
    tconf.ACC_MONITOR_TXN_DELTA_K = old_delta
    tconf.ACC_MONITOR_ENABLED = old_acc_monitor_enabled


def create_pre_prepare_params(state_root,
                              ledger_id=DOMAIN_LEDGER_ID,
                              txn_root=None,
                              timestamp=None,
                              bls_multi_sig=None,
                              view_no=0,
                              pool_state_root=None,
                              pp_seq_no=0,
                              inst_id=0,
                              audit_txn_root=None,
                              reqs=None):
    digest = Replica.batchDigest(reqs) if reqs is not None else "random digest"
    req_idrs = [req.key for req in reqs] if reqs is not None else ["random request"]
    params = [inst_id,
              view_no,
              pp_seq_no,
              timestamp or get_utc_epoch(),
              req_idrs,
              init_discarded(0),
              digest,
              ledger_id,
              state_root,
              txn_root or '1' * 32,
              0,
              True,
              pool_state_root or generate_state_root(),
              audit_txn_root or generate_state_root()]
    if bls_multi_sig:
        params.append(bls_multi_sig.as_list())
    return params


def create_pre_prepare_no_bls(state_root, view_no=0, pool_state_root=None, pp_seq_no=0, inst_id=0, audit_txn_root=None):
    params = create_pre_prepare_params(state_root=state_root,
                                       view_no=view_no,
                                       pool_state_root=pool_state_root,
                                       pp_seq_no=pp_seq_no,
                                       inst_id=inst_id,
                                       audit_txn_root=audit_txn_root)
    return PrePrepare(*params)


def create_commit_params(view_no, pp_seq_no, inst_id=0):
    return [inst_id, view_no, pp_seq_no]


def create_commit_no_bls_sig(req_key, inst_id=0):
    view_no, pp_seq_no = req_key
    params = create_commit_params(view_no, pp_seq_no, inst_id=inst_id)
    return Commit(*params)


def create_commit_with_bls_sig(req_key, bls_sig):
    view_no, pp_seq_no = req_key
    params = create_commit_params(view_no, pp_seq_no)
    params.append(bls_sig)
    return Commit(*params)


def create_commit_bls_sig(bls_bft, req_key, pre_prepare):
    view_no, pp_seq_no = req_key
    params = create_commit_params(view_no, pp_seq_no)
    params = bls_bft.update_commit(params, pre_prepare)
    return Commit(*params)


def create_prepare_params(view_no, pp_seq_no, state_root, inst_id=0):
    return [inst_id,
            view_no,
            pp_seq_no,
            get_utc_epoch(),
            "random digest",
            state_root,
            '1' * 32]


def create_prepare_from_pre_prepare(pre_prepare):
    params = [pre_prepare.instId,
              pre_prepare.viewNo,
              pre_prepare.ppSeqNo,
              pre_prepare.ppTime,
              pre_prepare.digest,
              pre_prepare.stateRootHash,
              pre_prepare.txnRootHash,
              pre_prepare.auditTxnRootHash]
    return Prepare(*params)


def create_prepare(req_key, state_root, inst_id=0):
    view_no, pp_seq_no = req_key
    params = create_prepare_params(view_no, pp_seq_no, state_root, inst_id=inst_id)
    return Prepare(*params)


def generate_state_root():
    return base58.b58encode(os.urandom(32)).decode("utf-8")


def init_discarded(value=None):
    """init discarded field with value and return message like representation"""
    discarded = []
    if value:
        discarded.append(value)
    return invalid_index_serializer.serialize(discarded, toBytes=False)


def incoming_3pc_msgs_count(nodes_count: int = 4) -> int:
    pre_prepare = 1  # Message from Primary
    prepares = nodes_count - 2  # Messages from all nodes exclude primary and self node
    commits = nodes_count - 1  # Messages from all nodes exclude  self node
    # The primary node receives the same number of messages. Doesn't get pre-prepare,
    # but gets one more prepare
    return pre_prepare + prepares + commits


def check_missing_pre_prepares(nodes, count):
    assert all(count <= len(replica.prePreparesPendingPrevPP)
               for replica in getNonPrimaryReplicas(nodes, instId=0))


class MockTimestamp:
    def __init__(self, value=datetime.utcnow()):
        self.value = value

    def __call__(self):
        return self.value


class MockTimer(QueueTimer):
    def __init__(self, start_time: int = 0):
        self._ts = MockTimestamp(start_time)
        QueueTimer.__init__(self, self._ts)

    def set_time(self, value):
        """
        Update time and run scheduled callbacks afterwards
        """
        self._ts.value = value
        self.service()

    def sleep(self, seconds):
        """
        Simulate sleeping for given amount of seconds, and run scheduled callbacks afterwards
        """
        self.set_time(self._ts.value + seconds)

    def advance(self):
        """
        Advance time to next scheduled callback and run that callback
        """
        if not self._events:
            return

        event = self._pop_event()
        self._ts.value = event.timestamp
        event.callback()

    def advance_until(self, value):
        """
        Advance time in steps until required value running scheduled callbacks in process
        """
        while self._events and self._next_timestamp() <= value:
            self.advance()
        self._ts.value = value

    def run_for(self, seconds):
        """
        Simulate running for given amount of seconds, running scheduled callbacks at required timestamps
        """
        self.advance_until(self._ts.value + seconds)

    def wait_for(self, condition: Callable[[], bool], timeout: Optional = None):
        """
        Advance time in steps until condition is reached, running scheduled callbacks in process
        Throws TimeoutError if fail to reach condition (under required timeout if defined)
        """
        deadline = self._ts.value + timeout if timeout else None
        while self._events and not condition():
            if deadline and self._next_timestamp() > deadline:
                raise TimeoutError("Failed to reach condition in required time")
            self.advance()

        if not condition():
            raise TimeoutError("Condition will be never reached")

    def run_to_completion(self):
        """
        Advance time in steps until nothing is scheduled
        """
        while self._events:
            self.advance()


class MockNetwork(ExternalBus):
    def __init__(self):
        super().__init__(self._send_message)
        self.sent_messages = []

    def _send_message(self, msg: Any, dst: ExternalBus.Destination):
        self.sent_messages.append((msg, dst))


def get_handler_by_type_wm(write_manager, h_type):
    for h_l in write_manager.request_handlers.values():
        for h in h_l:
            if isinstance(h, h_type):
                return h
