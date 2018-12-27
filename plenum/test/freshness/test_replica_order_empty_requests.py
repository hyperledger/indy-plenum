from plenum.common.constants import POOL_LEDGER_ID
from plenum.test.helper import assertExp
from plenum.test.test_node import getPrimaryReplica
from stp_core.loop.eventually import eventually


def test_order_empty_pre_prepare(looper, tconf, txnPoolNodeSet,
                                 sdk_wallet_client):
    assert all(node.master_replica.last_ordered_3pc == (0, 0) for node in txnPoolNodeSet)
    assert all(node.spylog.count(node.processOrdered) == 0 for node in txnPoolNodeSet)

    replica = getPrimaryReplica([txnPoolNodeSet[0]], instId=0)
    replica._do_send_3pc_batch(ledger_id=POOL_LEDGER_ID, is_empty=True)

    looper.run(eventually(
        lambda: assertExp(
            all(
                node.master_replica.last_ordered_3pc == (0, 1) for node in txnPoolNodeSet
            )
        )
    ))
    looper.run(eventually(
        lambda: assertExp(
            all(
                node.spylog.count(node.processOrdered) == 1 for node in txnPoolNodeSet
            )
        )
    ))
