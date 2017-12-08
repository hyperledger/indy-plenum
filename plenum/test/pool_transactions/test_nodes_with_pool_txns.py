import itertools
from copy import copy

import base58

from plenum.common.constants import CLIENT_STACK_SUFFIX, DATA, TARGET_NYM, \
    NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT
from plenum.common.signer_simple import SimpleSigner
from plenum.common.util import getMaxFailures, randomString
from plenum.test import waits
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    waitRejectWithReason, \
    waitReqNackFromPoolWithReason
from plenum.test.node_catchup.helper import ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import addNewClient, \
    addNewStewardAndNode, sendAddNewNode, add_2_nodes
from plenum.test.test_node import checkNodesConnected, \
    checkProtocolInstanceSetup
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

logger = getlogger()

# logged errors to ignore
whitelist = ['found legacy entry', "doesn't match", 'reconciling nodeReg',
             'missing', 'conflicts', 'matches', 'nodeReg',
             'conflicting address', 'unable to send message',
             'got error while verifying message']


# Whitelisting "got error while verifying message" since a node while not have
# initialised a connection for a new node by the time the new node's message
# reaches it

def testStewardCannotAddMoreThanOneNode(looper, txnPoolNodeSet, steward1,
                                        stewardWallet, tdir, tconf,
                                        allPluginsPath):
    newNodeName = "Epsilon"
    sendAddNewNode(tdir, tconf, newNodeName, steward1, stewardWallet)

    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, steward1,
                             'already has a node',
                             node.clientstack.name)


def testNonStewardCannotAddNode(looper, txnPoolNodeSet, client1,
                                wallet1, client1Connected, tdir,
                                tconf, allPluginsPath):
    newNodeName = "Epsilon"
    sendAddNewNode(tdir, tconf, newNodeName, client1, wallet1)
    for node in txnPoolNodeSet:
        waitRejectWithReason(
            looper, client1, 'is not a steward so cannot add a '
            'new node', node.clientstack.name)


def testClientConnectsToNewNode(
        looper,
        txnPoolNodeSet,
        tdir,
        client_tdir,
        tconf,
        steward1,
        stewardWallet,
        allPluginsPath):
    """
    A client should be able to connect to a newly added node
    """
    newStewardName = "testClientSteward" + randomString(3)
    newNodeName = "Epsilon"
    oldNodeReg = copy(steward1.nodeReg)
    newSteward, newStewardWallet, newNode = addNewStewardAndNode(looper,
                                                                 steward1, stewardWallet,
                                                                 newStewardName, newNodeName,
                                                                 tdir, client_tdir,
                                                                 tconf, allPluginsPath)
    txnPoolNodeSet.append(newNode)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    logger.debug("{} connected to the pool".format(newNode))

    def chkNodeRegRecvd():
        assert (len(steward1.nodeReg) - len(oldNodeReg)) == 1
        assert (newNode.name + CLIENT_STACK_SUFFIX) in steward1.nodeReg

    timeout = waits.expectedClientToPoolConnectionTimeout(len(txnPoolNodeSet))
    looper.run(eventually(chkNodeRegRecvd, retryWait=1, timeout=timeout))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)


def testAdd2NewNodes(looper, txnPoolNodeSet, tdir, client_tdir, tconf, steward1,
                     stewardWallet, allPluginsPath):
    """
    Add 2 new nodes to trigger replica addition and primary election
    """
    new_nodes = add_2_nodes(looper, txnPoolNodeSet, steward1, stewardWallet,
                            tdir, client_tdir, tconf, allPluginsPath)
    for n in new_nodes:
        logger.debug("{} connected to the pool".format(n))

    f = getMaxFailures(len(txnPoolNodeSet))

    def checkFValue():
        for node in txnPoolNodeSet:
            assert node.f == f
            assert len(node.replicas) == (f + 1)

    timeout = waits.expectedClientToPoolConnectionTimeout(len(txnPoolNodeSet))
    looper.run(eventually(checkFValue, retryWait=1, timeout=timeout))
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)


def testStewardCannotAddNodeWithOutFullFieldsSet(looper, tdir, tconf,
                                                 txnPoolNodeSet,
                                                 newAdHocSteward):
    """
    The case:
        Steward accidentally sends the NODE txn without full fields set.
    The expected result:
        Steward gets NAck response from the pool.
    """
    newNodeName = "Epsilon"

    newSteward, newStewardWallet = newAdHocSteward

    # case from the ticket
    def _renameNodePortField(op):
        op[DATA].update({NODE_PORT + ' ': op[DATA][NODE_PORT]})
        del op[DATA][NODE_PORT]

    sendAddNewNode(tdir, tconf, newNodeName, newSteward, newStewardWallet,
                   transformOpFunc=_renameNodePortField)
    waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, newSteward,
                                  "unknown field")

    for fn in (NODE_IP, CLIENT_IP, NODE_PORT, CLIENT_PORT):
        def _tnf(op): del op[DATA][fn]

        sendAddNewNode(tdir, tconf, newNodeName, newSteward, newStewardWallet,
                       transformOpFunc=_tnf)
        # wait NAcks with exact message. it does not works for just 'is missed'
        # because the 'is missed' will check only first few cases
        waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, newSteward,
                                      "unknown field")


def testNodesConnect(txnPoolNodeSet):
    pass


def testNodesReceiveClientMsgs(looper, txnPoolNodeSet, wallet1, client1,
                               client1Connected):
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client1,
                                                  *txnPoolNodeSet)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)


def testAddNewClient(looper, txnPoolNodeSet, steward1, stewardWallet):
    wallet = addNewClient(None, looper, steward1,
                          stewardWallet, randomString())

    def chk():
        for node in txnPoolNodeSet:
            assert wallet.defaultId in \
                   node.clientAuthNr.core_authenticator.clients

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))


def testStewardCannotAddNodeWithNonBase58VerKey(looper, tdir, tconf,
                                                txnPoolNodeSet,
                                                newAdHocSteward):
    """
    The Case:
        Steward accidentally sends the NODE txn with a non base58 verkey.
    The expected result:
        Steward gets NAck response from the pool.
    """
    # create a new steward
    newSteward, newStewardWallet = newAdHocSteward

    newNodeName = "Epsilon"

    # get hex VerKey
    sigseed = randomString(32).encode()
    nodeSigner = SimpleSigner(seed=sigseed)
    b = base58.b58decode(nodeSigner.identifier)
    hexVerKey = bytearray(b).hex()

    def _setHexVerkey(op):
        op[TARGET_NYM] = hexVerKey
        return op

    sendAddNewNode(tdir, tconf, newNodeName, newSteward, newStewardWallet,
                   transformOpFunc=_setHexVerkey)
    waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, newSteward,
                                  'should not contain the following chars')


def testStewardCannotAddNodeWithInvalidHa(looper, tdir, tconf,
                                          txnPoolNodeSet,
                                          newAdHocSteward):
    """
    The case:
        Steward accidentally sends the NODE txn with an invalid HA.
    The expected result:
        Steward gets NAck response from the pool.
    """
    newNodeName = "Epsilon"

    newSteward, newStewardWallet = newAdHocSteward

    # a sequence of the test cases for each field
    tests = itertools.chain(
        itertools.product(
            (NODE_IP, CLIENT_IP), ('127.0.0.1 ', '256.0.0.1', '0.0.0.0')
        ),
        itertools.product(
            (NODE_PORT, CLIENT_PORT), ('foo', '9700',
                                       0, 65535 + 1, 4351683546843518184)
        ),
    )

    for field, value in tests:
        # create a transform function for each test
        def _tnf(op): op[DATA].update({field: value})

        sendAddNewNode(tdir, tconf, newNodeName, newSteward, newStewardWallet,
                       transformOpFunc=_tnf)
        # wait NAcks with exact message. it does not works for just 'is invalid'
        # because the 'is invalid' will check only first few cases
        waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, newSteward,
                                      "invalid network ip address")
