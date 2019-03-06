from contextlib import contextmanager

from plenum.test.pool_transactions.helper import demote_node

nodeCount = 8


@contextmanager
def delay_ordered(node):
    ordereds = []
    old_processing = node.try_processing_ordered
    node.try_processing_ordered = lambda msg: ordereds.append(msg)
    yield node
    node.try_processing_ordered = old_processing
    for msg in ordereds:
        node.replicas[msg.instId].outBox.append(msg)


def test_future_primaries_works(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards):
    with delay_ordered(txnPoolNodeSet[4]):
        demote_node(looper, sdk_wallet_stewards[3], sdk_pool_handle, txnPoolNodeSet[3])
        assert txnPoolNodeSet[4].primaries != txnPoolNodeSet[5].primaries
        assert txnPoolNodeSet[4].future_primaries.primaries == txnPoolNodeSet[5].primaries


def test_future_primaries_reset():
    pass
