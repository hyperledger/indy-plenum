import pytest
import zmq

from plenum.test.client.helper import create_zmq_connection
from plenum.test.logging.conftest import logsearch
from stp_core.loop.eventually import eventually


@pytest.fixture(params=[zmq.REQ])
def zmq_connection(test_node, request, looper):
    sock = create_zmq_connection(test_node, request.param)

    yield sock
    sock.close(linger=0)
    sock = None
    test_node.stop()
    looper.removeProdable(test_node)


def test_send_using_not_dealer_socket(zmq_connection, test_node, looper, sdk_wallet_client, logsearch):
    logs, _ = logsearch(files=['zstack.py'], msgs=['Got too many values for unpack'])

    def check_reply():
        assert logs

    looper.add(test_node)
    msg = "{ \"op\": \"LEDGER_STATUS\", \"txnSeqNo\": 0, \"merkleRoot\": \"\", \"ledgerId\": 0, \"ppSeqNo\": null, \"viewNo\": null, \"protocolVersion\": 2}"
    zmq_connection.send_string(msg)
    looper.run(eventually(check_reply))
