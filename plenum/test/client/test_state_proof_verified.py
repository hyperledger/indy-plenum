from plenum.common.util import getMaxFailures
from plenum.test import waits
from plenum.test.helper import sendRandomRequest, \
    checkResponseCorrectnessFromNodes
from stp_core.loop.eventually import eventually
# noinspection PyUnresolvedReferences
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected


nodeCount = 4
nodes_wth_bls = 4

F = getMaxFailures(nodeCount)


def check_proved_reply_received(client, identifier, request_id):
    client.take_one_quorumed = lambda a, b: False
    reply, _ = client.getReply(identifier, request_id)
    assert reply


def test_state_proof_checked_in_client_request(looper, txnPoolNodeSet,
                                               client1, client1Connected, wallet1):
    """
    Checks that client cat use state proofs instead of quorum for replies.

    To implement this test state proof is added to replies for
    'greeting' requests in TestNode. Parsing added to TestClient.
    """
    request = sendRandomRequest(wallet1, client1)
    responseTimeout = waits.expectedTransactionExecutionTime(nodeCount)
    looper.run(
        eventually(check_proved_reply_received,
                   client1, request.identifier, request.reqId,
                   retryWait=1, timeout=responseTimeout))
    checkResponseCorrectnessFromNodes(client1.inBox, request.reqId, F)
