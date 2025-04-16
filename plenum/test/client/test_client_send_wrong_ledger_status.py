import pytest
import zmq

from plenum.test.client.helper import create_zmq_connection


@pytest.fixture()
def client(vdr_test_node):
    sock = create_zmq_connection(vdr_test_node, zmq.DEALER)

    yield sock
    sock.close(linger=0)
    sock = None
    vdr_test_node.stop()


def test_client_send_wrong_ledger_status(client, looper, vdr_test_node):
    looper.add(vdr_test_node)
    wrong_msg = "{ \"op\": \"LEDGER_STATUS\", \"txnSeqNo\": 0, \"merkleRoot\": null, \"ledgerId\": 0, \"ppSeqNo\": null, \"viewNo\": null, \"protocolVersion\": 2}"
    client.send_string(wrong_msg)
    # ugly hack... needs to run several steps for looper
    looper.runFor(5)
    res = client.poll(timeout=5 * 1000)
    assert res

    res = client.recv_string()
    assert "client request invalid" in res
