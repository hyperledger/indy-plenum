from plenum.common.types import f

from plenum.test.helper import sdk_send_batches_of_random_and_check

nodeCount = 7


def test_bls_not_depend_on_node_reg(looper, txnPoolNodeSet, sdk_pool_handle,
                                    sdk_wallet_client):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_client, 1, 2)

    node = txnPoolNodeSet[2]
    last_pre_prepare = \
        node.master_replica.prePrepares[node.master_replica.last_ordered_3pc]

    bls = getattr(last_pre_prepare, f.BLS_MULTI_SIG.nm)

    # Get random participant
    node_name = next(iter(bls[1]))

    # We've removed one of the nodes from another node's log
    del node.nodeReg[node_name]

    # Still we can validate Preprepare
    assert node.master_replica._bls_bft_replica._bls_bft.bls_key_register.get_key_by_name(node_name)
