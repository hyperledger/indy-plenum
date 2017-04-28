import types

import pytest

from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.view_change.helper import elongate_view_change_timeout, \
    ensure_view_change


@pytest.fixture(scope="module")
def tconf(tconf, request):
    return elongate_view_change_timeout(tconf, request, by=10)


def test_view_change_happens_after_all_nodes_send(txnPoolNodeSet, looper,
                                                  tconf, client1, wallet1,
                                                  client1Connected):
    # View change should not wait for timeout once all nodes send view
    # change message
    ensure_view_change(looper, txnPoolNodeSet, client1, wallet1)

    for node in txnPoolNodeSet:
        first_recv = node.spylog.getAll(node.processInstanceChange.__name__)[-1]
        view_change = node.spylog.getAll(node.startViewChange.__name__)[0]
        # View change happens in before view change timeout expires
        assert (view_change.starttime - first_recv.starttime) < tconf.ViewChangeTimeout


def test_view_change_happens_post_timeout(txnPoolNodeSet, looper, tconf,
                                          client1, wallet1, client1Connected):
    # View change should not happen unless the timeout expires, since all nodes
    # would not send view change message

    def dont_send(self, view_no, suspicion=None):
        pass

    # One of the node does not send view change message
    txnPoolNodeSet[0].sendInstanceChange = types.MethodType(dont_send,
                                                            txnPoolNodeSet[0])

    ensure_view_change(looper, txnPoolNodeSet, client1, wallet1)
    for node in txnPoolNodeSet:
        first_recv = node.spylog.getAll(node.processInstanceChange.__name__)[-1]
        view_change = node.spylog.getAll(node.startViewChange.__name__)[0]
        # View change happens in after view change timeout expires
        assert (view_change.starttime - first_recv.starttime) > tconf.ViewChangeTimeout
