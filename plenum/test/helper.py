import itertools
import os
import random
import string
from _signal import SIGINT
from functools import partial
from itertools import permutations
from shutil import copyfile
from sys import executable
from time import sleep

from psutil import Popen
from raet.raeting import TrnsKind, PcktKind
from typing import Tuple, Iterable, Dict, Optional, NamedTuple, \
    List, Any, Sequence
from typing import Union

from plenum.client.client import Client
from plenum.client.wallet import Wallet
from plenum.common.log import getlogger
from stp_core.loop.looper import Looper
from plenum.common.request import Request
from plenum.common.constants import REPLY, REQACK, TXN_ID, REQNACK, OP_FIELD_NAME
from plenum.common.types import Reply, f, PrePrepare
from plenum.common.util import getMaxFailures, \
    checkIfMoreThanFSameItems
from plenum.config import poolTransactionsFile, domainTransactionsFile
from stp_core.loop.eventually import eventuallyAll, eventually

from stp_core.network.util import checkPortAvailable
from plenum.server.node import Node
from plenum.test.msgs import randomMsg
from plenum.test.spy_helpers import getLastClientReqReceivedForNode, getAllArgs, \
    getAllReturnVals
from plenum.test.test_client import TestClient, genTestClient
from plenum.test.test_node import TestNode, TestReplica, TestNodeSet, \
    checkPoolReady, checkNodesConnected, ensureElectionsDone, NodeRef
from plenum.test import waits


DelayRef = NamedTuple("DelayRef", [
    ("op", Optional[str]),
    ("frm", Optional[str])])

RaetDelay = NamedTuple("RaetDelay", [
    ("tk", Optional[TrnsKind]),
    ("pk", Optional[PcktKind]),
    ("fromPort", Optional[int])])


logger = getlogger()


# noinspection PyUnresolvedReferences


def ordinal(n):
    return "%d%s" % (
        n, "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])


def checkSufficientRepliesRecvd(receivedMsgs: Iterable, reqId: int,
                                fValue: int):
    receivedReplies = getRepliesFromClientInbox(receivedMsgs, reqId)
    logger.debug("received replies for reqId {}: {}".
                 format(reqId, receivedReplies))
    assert len(receivedReplies) > fValue, "Received {} replies but expected " \
                                          "at-least {} for reqId {}".\
        format(len(receivedReplies), fValue+1, reqId)
    result = checkIfMoreThanFSameItems([reply[f.RESULT.nm] for reply in
                                        receivedReplies], fValue)
    assert result

    assert all([r[f.RESULT.nm][f.REQ_ID.nm] == reqId for r in receivedReplies])
    return result
    # TODO add test case for what happens when replies don't have the same data


def checkSufficientRepliesForRequests(looper,
                                      client,
                                      requests,
                                      fVal=None,
                                      timeoutPerReq=None):
    nodeCount = len(client.nodeReg)
    fVal = fVal or getMaxFailures(nodeCount)

    if timeoutPerReq is None:
        timeoutPerReq = waits.expectedTransactionExecutionTime(nodeCount)

    totalTimeout = timeoutPerReq * len(requests)

    coros = []
    for request in requests:
        coros.append(partial(checkSufficientRepliesRecvd,
                             client.inBox,
                             request.reqId,
                             fVal))

    looper.run(eventuallyAll(*coros,
                             retryWait=1,
                             totalTimeout=totalTimeout))


def sendReqsToNodesAndVerifySuffReplies(looper: Looper, wallet: Wallet,
                                        client: TestClient,
                                        numReqs: int, fVal: int=None,
                                        timeoutPerReq: float=None):
    nodeCount = len(client.nodeReg)
    fVal = fVal or getMaxFailures(nodeCount)
    timeoutPerReq = timeoutPerReq or 5 * nodeCount

    requests = sendRandomRequests(wallet, client, numReqs)
    checkSufficientRepliesForRequests(looper, client, requests, fVal,
                                      timeoutPerReq)
    return requests


# noinspection PyIncorrectDocstring
def checkResponseCorrectnessFromNodes(receivedMsgs: Iterable, reqId: int,
                                      fValue: int) -> bool:
    """
    the client must get at least :math:`2f+1` responses
    """
    msgs = [(msg[f.RESULT.nm][f.REQ_ID.nm], msg[f.RESULT.nm][TXN_ID]) for msg in
            getRepliesFromClientInbox(receivedMsgs, reqId)]
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
                                           "expected length was {}".\
        format(len(collection), expectedLength)


def assertEquality(observed: Any, expected: Any):
    assert observed == expected, "Observed value was {} but expected value " \
                                 "was {}".format(observed, expected)


def checkNodesReadyForRequest(looper: Looper, nodes: Sequence[TestNode],
                              timeout: int = 20):
    checkPoolReady(looper, nodes, timeout)
    # checkNodesCanRespondToClients(nodes)


def setupNodesAndClient(looper: Looper, nodes: Sequence[TestNode], nodeReg=None,
                        tmpdir=None):
    looper.run(checkNodesConnected(nodes))
    ensureElectionsDone(looper=looper, nodes=nodes)
    return setupClient(looper, nodes, nodeReg=nodeReg, tmpdir=tmpdir)


def setupClient(looper: Looper,
                nodes: Sequence[TestNode] = None,
                nodeReg=None,
                tmpdir=None,
                identifier=None,
                verkey=None):
    client1, wallet = genTestClient(nodes=nodes,
                                    nodeReg=nodeReg,
                                    tmpdir=tmpdir,
                                    identifier=identifier,
                                    verkey=verkey)
    looper.add(client1)
    looper.run(client1.ensureConnectedToNodes())
    return client1, wallet


def setupClients(count: int,
                 looper: Looper,
                 nodes: Sequence[TestNode] = None,
                 nodeReg=None,
                 tmpdir=None):
    wallets = {}
    clients = {}
    for i in range(count):
        name = "test-wallet-{}".format(i)
        wallet = Wallet(name)
        idr, _ = wallet.addIdentifier()
        verkey = wallet.getVerkey(idr)
        client, _ = setupClient(looper,
                                nodes,
                                nodeReg,
                                tmpdir,
                                identifier=idr,
                                verkey=verkey)
        clients[client.name] = client
        wallets[client.name] = wallet
    return clients, wallets


# noinspection PyIncorrectDocstring
async def aSetupClient(looper: Looper,
                       nodes: Sequence[TestNode] = None,
                       nodeReg=None,
                       tmpdir=None):
    """
    async version of above
    """
    client1 = genTestClient(nodes=nodes,
                            nodeReg=nodeReg,
                            tmpdir=tmpdir)
    looper.add(client1)
    await client1.ensureConnectedToNodes()
    return client1


def randomOperation():
    return {
        "type": "buy",
        "amount": random.randint(10, 100)
    }


def sendRandomRequest(wallet: Wallet, client: Client):
    return sendRandomRequests(wallet, client, 1)[0]


def sendRandomRequests(wallet: Wallet, client: Client, count: int):
    logger.debug('{} random requests will be sent'.format(count))
    reqs = [wallet.signOp(randomOperation()) for _ in range(count)]
    return client.submitReqs(*reqs)


def buildCompletedTxnFromReply(request, reply: Reply) -> Dict:
    txn = request.operation
    txn.update(reply)
    return txn


async def msgAll(nodes: TestNodeSet):
    # test sending messages from every node to every other node
    # TODO split send and check so that the messages can be sent concurrently
    for p in permutations(nodes.nodeNames, 2):
        await sendMsgAndCheck(nodes, p[0], p[1], timeout=3)


async def sendMsgAndCheck(nodes: TestNodeSet,
                          frm: NodeRef,
                          to: NodeRef,
                          msg: Optional[Tuple]=None,
                          timeout: Optional[int]=15):
    logger.debug("Sending msg from {} to {}".format(frm, to))
    msg = msg if msg else randomMsg()
    sender = nodes.getNode(frm)
    rid = sender.nodestack.getRemote(nodes.getNodeName(to)).uid
    sender.nodestack.send(msg, rid)
    await eventually(checkMsg, msg, nodes, to,
                     retryWait=.1,
                     timeout=timeout,
                     ratchetSteps=10)


def checkMsg(msg, nodes, to, method: str = None):
    allMsgs = nodes.getAllMsgReceived(to, method)
    assert msg in allMsgs


def addNodeBack(nodeSet: TestNodeSet,
                looper: Looper,
                nodeName: str) -> TestNode:
    node = nodeSet.addNode(nodeName)
    looper.add(node)
    return node


# def checkMethodCalled(node: TestNode,
#                       method: str,
#                       args: Tuple):
#     assert node.spylog.getLastParams(method) == args


def checkPropagateReqCountOfNode(node: TestNode, identifier: str, reqId: int):
    key = identifier, reqId
    assert key in node.requests
    assert len(node.requests[key].propagates) >= node.f + 1


def requestReturnedToNode(node: TestNode, identifier: str, reqId: int,
                               instId: int):
    params = getAllArgs(node, node.processOrdered)
    # Skipping the view no and time from each ordered request
    recvdOrderedReqs = [p['ordered'][:1] + p['ordered'][2:-1] for p in params]
    expected = (instId, identifier, reqId)
    return expected in recvdOrderedReqs


def checkRequestReturnedToNode(node: TestNode, identifier: str, reqId: int,
                               instId: int):
    assert requestReturnedToNode(node, identifier, reqId, instId)


def checkPrePrepareReqSent(replica: TestReplica, req: Request):
    prePreparesSent = getAllArgs(replica, replica.doPrePrepare)
    expected = req.reqDigest
    assert expected in [p["reqDigest"] for p in prePreparesSent]


def checkPrePrepareReqRecvd(replicas: Iterable[TestReplica],
                            expectedRequest: PrePrepare):
    for replica in replicas:
        params = getAllArgs(replica, replica.canProcessPrePrepare)
        assert expectedRequest[:-1] in [p['pp'][:-1] for p in params]


def checkPrepareReqSent(replica: TestReplica, identifier: str, reqId: int):
    paramsList = getAllArgs(replica, replica.canSendPrepare)
    rv = getAllReturnVals(replica,
                          replica.canSendPrepare)
    for params in paramsList:
        req = params['request']
        assert req.identifier == identifier
        assert req.reqId == reqId
    assert all(rv)


def checkSufficientPrepareReqRecvd(replica: TestReplica, viewNo: int,
                                   ppSeqNo: int):
    key = (viewNo, ppSeqNo)
    assert key in replica.prepares
    assert len(replica.prepares[key][1]) >= 2 * replica.f


def checkSufficientCommitReqRecvd(replicas: Iterable[TestReplica], viewNo: int,
                                  ppSeqNo: int):
    for replica in replicas:
        key = (viewNo, ppSeqNo)
        assert key in replica.commits
        received = len(replica.commits[key][1])
        minimum = 2 * replica.f
        assert received > minimum


def checkReqAck(client, node, idr, reqId, update: Dict[str, str]=None):
    rec = {OP_FIELD_NAME: REQACK, f.REQ_ID.nm: reqId, f.IDENTIFIER.nm: idr}
    if update:
        rec.update(update)
    expected = (rec, node.clientstack.name)
    # More than one matching message could be present in the client's inBox
    # since client on not receiving request under timeout might have retried
    # the request
    assert client.inBox.count(expected) > 0


def checkReqNack(client, node, idr, reqId, update: Dict[str, str]=None):
    rec = {OP_FIELD_NAME: REQNACK, f.REQ_ID.nm: reqId, f.IDENTIFIER.nm: idr}
    if update:
        rec.update(update)
    expected = (rec, node.clientstack.name)
    # More than one matching message could be present in the client's inBox
    # since client on not receiving request under timeout might have retried
    # the request
    assert client.inBox.count(expected) > 0


def checkReplyCount(client, idr, reqId, count):
    senders = set()
    for msg, sdr in client.inBox:
        if msg[OP_FIELD_NAME] == REPLY and \
                        msg[f.RESULT.nm][f.IDENTIFIER.nm] == idr and \
                        msg[f.RESULT.nm][f.REQ_ID.nm] == reqId:
            senders.add(sdr)
    assertLength(senders, count)


def checkReqNackWithReason(client, reason: str, sender: str):
    found = False
    for msg, sdr in client.inBox:
        if msg[OP_FIELD_NAME] == REQNACK and reason in msg.get(f.REASON.nm, "")\
                and sdr == sender:
            found = True
            break
    assert found


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
    assert len(viewNos) == 1
    vNo, = viewNos
    if expectedViewNo:
        assert vNo == expectedViewNo, ','.join(['{} -> Ratio: {}'.format(
            node.name, node.monitor.masterThroughputRatio()) for node in nodes])
    return vNo


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
        if 'reason' in entry.params and reasonPat in entry.params['reason']:
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
    for l1, l2 in permutations(ledgers, 2):
        checkLedgerEquality(l1, l2)


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


def initDirWithGenesisTxns(dirName, tconf, tdirWithPoolTxns=None,
                           tdirWithDomainTxns=None):
    os.makedirs(dirName, exist_ok=True)
    if tdirWithPoolTxns:
        copyfile(os.path.join(tdirWithPoolTxns, poolTransactionsFile),
                 os.path.join(dirName, tconf.poolTransactionsFile))
    if tdirWithDomainTxns:
        copyfile(os.path.join(tdirWithDomainTxns, domainTransactionsFile),
                 os.path.join(dirName, tconf.domainTransactionsFile))


def stopNodes(nodes: List[TestNode], looper=None, ensurePortsFreedUp=True):
    if ensurePortsFreedUp:
        assert looper, 'Need a looper to make sure ports are freed up'

    for node in nodes:
        node.stop()

    if ensurePortsFreedUp:
        ports = [[n.nodestack.ha[1], n.clientstack.ha[1]] for n in nodes]
        waitUntillPortIsAvailable(looper, ports)


def waitUntillPortIsAvailable(looper, ports):
    ports = itertools.chain(*ports)

    def chk():
        for port in ports:
            checkPortAvailable(("", port))

    looper.run(eventually(chk, retryWait=.5))


def run_script(script, *args):
    s = os.path.join(os.path.dirname(__file__), '../../scripts/' + script)
    command = [executable, s]
    command.extend(args)

    with Popen([executable, s]) as p:
        sleep(4)
        p.send_signal(SIGINT)
        p.wait(timeout=1)
        assert p.poll() == 0, 'script failed'
