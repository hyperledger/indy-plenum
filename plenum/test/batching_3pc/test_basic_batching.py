import types
import pytest
from plenum.common.exceptions import UnauthorizedClientRequest, RequestRejectedException
from plenum.test.batching_3pc.helper import checkNodesHaveSameRoots
from plenum.test.helper import sdk_send_random_requests, sdk_get_and_check_replies
from plenum.common.exceptions import InvalidClientRequest
from plenum.test.helper import sdk_sign_request_from_dict, sdk_send_random_and_check
from plenum.common.request import Request


def testRequestStaticValidation(tconf, looper, txnPoolNodeSet,
                                sdk_wallet_client):
    """
    Check that for requests which fail static validation, REQNACK is sent
    :return:
    """
    node = txnPoolNodeSet[0]
    req = sdk_sign_request_from_dict(looper, sdk_wallet_client, {'something': 'nothing'})
    req = Request(**req)
    with pytest.raises(InvalidClientRequest):
        node.doStaticValidation(req)


def test3PCOverBatchWithThresholdReqs(tconf, looper, txnPoolNodeSet,
                                      sdk_wallet_client, sdk_pool_handle):
    """
    Check that 3 phase commit happens when threshold number of requests are
    received and propagated.
    :return:
    """
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, tconf.Max3PCBatchSize)


def test3PCOverBatchWithLessThanThresholdReqs(tconf, looper, txnPoolNodeSet,
                                              sdk_wallet_client, sdk_pool_handle):
    """
    Check that 3 phase commit happens when threshold number of requests are
    not received but threshold time has passed
    :return:
    """
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, tconf.Max3PCBatchSize - 1)


def testTreeRootsCorrectAfterEachBatch(tconf, looper, txnPoolNodeSet,
                                       sdk_pool_handle, sdk_wallet_client):
    """
    Check if both state root and txn tree root are correct and same on each
    node after each batch
    :return:
    """
    # Send 1 batch
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, tconf.Max3PCBatchSize)
    checkNodesHaveSameRoots(txnPoolNodeSet)

    # Send 2 batches
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2 * tconf.Max3PCBatchSize)
    checkNodesHaveSameRoots(txnPoolNodeSet)


def testRequestDynamicValidation(tconf, looper, txnPoolNodeSet,
                                 sdk_pool_handle, sdk_wallet_client):
    """
    Check that for requests which fail dynamic (state based) validation,
    REJECT is sent to the client
    :return:
    """
    origMethods = []
    names = {node.name: 0 for node in txnPoolNodeSet}

    def rejectingMethod(self, req, pp_time):
        names[self.name] += 1
        # Raise rejection for last request of batch
        if tconf.Max3PCBatchSize - names[self.name] == 0:
            raise UnauthorizedClientRequest(req.identifier,
                                            req.reqId,
                                            'Simulated rejection')

    for node in txnPoolNodeSet:
        origMethods.append(node.doDynamicValidation)
        node.doDynamicValidation = types.MethodType(rejectingMethod, node)

    reqs = sdk_send_random_requests(looper, sdk_pool_handle,
                                    sdk_wallet_client,
                                    tconf.Max3PCBatchSize)
    sdk_get_and_check_replies(looper, reqs[:-1])
    with pytest.raises(RequestRejectedException) as e:
        sdk_get_and_check_replies(looper, reqs[-1:])

    assert 'Simulated rejection' in e._excinfo[1].args[0]

    for i, node in enumerate(txnPoolNodeSet):
        node.doDynamicValidation = origMethods[i]
