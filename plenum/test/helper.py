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
from typing import Tuple, Iterable, Dict, Optional, List, Any, Sequence, Union

import pytest
from indy.pool import set_protocol_version
from plenum.config import Max3PCBatchWait
from psutil import Popen
import json
import asyncio

from indy.ledger import sign_and_submit_request, sign_request, submit_request
from indy.error import ErrorCode, IndyError

from ledger.genesis_txn.genesis_txn_file_util import genesis_txn_file
from plenum.client.client import Client
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
from plenum.test import waits
from plenum.test.msgs import randomMsg
from plenum.test.spy_helpers import getLastClientReqReceivedForNode, getAllArgs, getAllReturnVals, \
    getAllMsgReceivedForNode
from plenum.test.test_node import TestNode, TestReplica, \
    getPrimaryReplica
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventuallyAll, eventually
from stp_core.loop.looper import Looper
from stp_core.network.util import checkPortAvailable

logger = getlogger()


# noinspection PyUnresolvedReferences


def ordinal(n):
    return "%d%s" % (
        n, "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])


def check_sufficient_replies_received(client: Client,
                                      identifier,
                                      request_id):
    reply, _ = client.getReply(identifier, request_id)
    full_request_id = "({}:{})".format(identifier, request_id)
    if reply is not None:
        logger.debug("got confirmed reply for {}: {}"
                     .format(full_request_id, reply))
        return reply
    all_replies = getRepliesFromClientInbox(client.inBox, request_id)
    logger.debug("there are {} replies for request {}, "
                 "but expected at-least {}, "
                 "or one with valid proof: "
                 .format(len(all_replies),
                         full_request_id,
                         client.quorums.reply.value,
                         all_replies))
    raise AssertionError("There is no proved reply and no "
                         "quorum achieved for request {}"
                         .format(full_request_id))


# TODO: delete after removal from node
def waitForSufficientRepliesForRequests(looper,
                                        client,
                                        *,  # To force usage of names
                                        requests,
                                        customTimeoutPerReq=None,
                                        add_delay_to_timeout: float = 0,
                                        override_timeout_limit=False,
                                        total_timeout=None):
    """
    Checks number of replies for given requests of specific client and
    raises exception if quorum not reached at least for one

    :requests: list of requests; mutually exclusive with 'requestIds'
    :requestIds:  list of request ids; mutually exclusive with 'requests'
    :returns: nothing
    """
    node_count = len(client.nodeReg)
    if not total_timeout:
        timeout_per_request = customTimeoutPerReq or \
                              waits.expectedTransactionExecutionTime(node_count)
        timeout_per_request += add_delay_to_timeout
        # here we try to take into account what timeout for execution
        # N request - total_timeout should be in
        # timeout_per_request < total_timeout < timeout_per_request * N
        # we cannot just take (timeout_per_request * N) because it is so huge.
        # (for timeout_per_request=5 and N=10, total_timeout=50sec)
        # lets start with some simple formula:
        total_timeout = (1 + len(requests) / 10) * timeout_per_request
    coros = [partial(check_sufficient_replies_received,
                     client,
                     request.identifier,
                     request.reqId)
             for request in requests]
    chk_all_funcs(looper, coros,
                  retry_wait=1,
                  timeout=total_timeout,
                  override_eventually_timeout=override_timeout_limit)


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


def getPendingRequestsForReplica(replica: TestReplica, requestType: Any):
    return [item[0] for item in replica.postElectionMsgs if
            isinstance(item[0], requestType)]


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
        "type": "buy",
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
    assert [req.digest, ] in \
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
    assert [key] in args
    idx = args.index([key])
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


def assertFunc(func):
    assert func()


def checkLedgerEquality(ledger1, ledger2):
    assertLength(ledger1, ledger2.size)
    assertEquality(ledger1.root_hash, ledger2.root_hash)


def checkAllLedgersEqual(*ledgers):
    for l1, l2 in combinations(ledgers, 2):
        checkLedgerEquality(l1, l2)


def checkStateEquality(state1, state2):
    assertEquality(state1.as_dict, state2.as_dict)
    assertEquality(state1.committedHeadHash, state2.committedHeadHash)
    assertEquality(state1.committedHead, state2.committedHead)


def check_seqno_db_equality(db1, db2):
    assert db1.size == db2.size, \
        "{} != {}".format(db1.size, db2.size)
    assert {bytes(k): bytes(v) for k, v in db1._keyValueStorage.iterator()} == \
           {bytes(k): bytes(v) for k, v in db2._keyValueStorage.iterator()}


def check_last_ordered_3pc(node1, node2):
    master_replica_1 = node1.master_replica
    master_replica_2 = node2.master_replica
    assert master_replica_1.last_ordered_3pc == master_replica_2.last_ordered_3pc, \
        "{} != {}".format(master_replica_1.last_ordered_3pc,
                          master_replica_2.last_ordered_3pc)
    return master_replica_1.last_ordered_3pc


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


def send_pre_prepare(view_no, pp_seq_no, wallet, nodes,
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
                   signature=req[f.SIG.nm]
                   if req.__contains__(f.SIG.nm) else None,
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
                         tdir, plugin_paths, node_ha=None, client_ha=None):
    config_helper = node_config_helper_class(name, conf, chroot=tdir)
    return test_node_class(name,
                           config_helper=config_helper,
                           config=conf,
                           pluginPaths=plugin_paths,
                           ha=node_ha,
                           cliha=client_ha)


# ####### SDK


def sdk_gen_request(operation, protocol_version=CURRENT_PROTOCOL_VERSION,
                    identifier=None, **kwargs):
    # Question: Why this method is called sdk_gen_request? It does not use
    # the indy-sdk
    return Request(operation=operation, reqId=random.randint(10, 1000000000),
                   protocolVersion=protocol_version, identifier=identifier,
                   **kwargs)


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


def sdk_sign_request_strings(looper, sdk_wallet, reqs: Sequence):
    wallet_h, did = sdk_wallet
    reqs_str = [json.dumps(req) for req in reqs]
    reqs = [looper.loop.run_until_complete(sign_request(wallet_h, did, req))
            for req in reqs_str]
    return reqs


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

    sdk_replies = []
    for _ in range(num_batches - 1):
        sdk_replies.extend(sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool, sdk_wallet,
                                                     num_reqs // num_batches, **kwargs))
    rem = num_reqs % num_batches
    if rem == 0:
        rem = num_reqs // num_batches
    sdk_replies.extend(sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool, sdk_wallet, rem, **kwargs))
    return sdk_replies


def sdk_send_batches_of_random(looper, txnPoolNodeSet, sdk_pool, sdk_wallet,
                               num_reqs, num_batches=1, timeout=Max3PCBatchWait):
    if num_reqs < num_batches:
        raise BaseException(
            'sdk_send_batches_of_random_and_check method assumes that `num_reqs` <= num_batches*MaxbatchSize')
    if num_batches == 1:
        req = sdk_send_random_requests(looper, sdk_pool, sdk_wallet, num_reqs)
        looper.runFor(timeout)
        return req

    sdk_reqs = []
    for _ in range(num_batches - 1):
        sdk_reqs.extend(sdk_send_random_requests(looper, sdk_pool, sdk_wallet,
                                                 num_reqs // num_batches))
        looper.runFor(timeout)
    return sdk_reqs


def sdk_sign_request_from_dict(looper, sdk_wallet, op, reqId=None):
    wallet_h, did = sdk_wallet
    reqId = reqId or random.randint(10, 100000)
    request = Request(operation=op, reqId=reqId,
                      protocolVersion=CURRENT_PROTOCOL_VERSION, identifier=did)
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
    return Request(identifier=json_req['identifier'],
                   reqId=json_req['reqId'],
                   operation=json_req['operation'],
                   signature=json_req['signature'] if 'signature' in json_req else None,
                   protocolVersion=json_req['protocolVersion'] if 'protocolVersion' in json_req else None)


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
def view_change_timeout(tconf, vc_timeout, catchup_timeout=None, propose_timeout=None):
    old_catchup_timeout = tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    old_view_change_timeout = tconf.VIEW_CHANGE_TIMEOUT
    old_propose_timeout = tconf.INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = \
        0.6 * vc_timeout if catchup_timeout is None else catchup_timeout
    tconf.VIEW_CHANGE_TIMEOUT = vc_timeout
    tconf.INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT = vc_timeout if propose_timeout is None else propose_timeout
    yield tconf
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = old_catchup_timeout
    tconf.VIEW_CHANGE_TIMEOUT = old_view_change_timeout
    tconf.INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT = old_propose_timeout


@contextmanager
def max_3pc_batch_limits(tconf, size, wait=10000):
    old_size = tconf.Max3PCBatchSize
    old_wait = tconf.Max3PCBatchWait
    tconf.Max3PCBatchSize = size
    tconf.Max3PCBatchWait = wait
    yield tconf
    tconf.Max3PCBatchSize = old_size
    tconf.Max3PCBatchWait = old_wait
