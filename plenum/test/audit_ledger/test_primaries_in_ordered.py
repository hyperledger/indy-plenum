from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root


def test_primaries_in_ordered_from_audit(test_node):

    pre_prepare = create_pre_prepare_no_bls(state_root=generate_state_root(), pp_seq_no=1)
    replica = test_node.master_replica
    key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
    replica.prePrepares[key] = pre_prepare
    test_node.primaries = ["Alpha", "Beta"]
    three_pc_batch = ThreePcBatch.from_pre_prepare(pre_prepare=pre_prepare,
                                                   valid_txn_count=len(pre_prepare.reqIdr),
                                                   state_root=pre_prepare.stateRootHash,
                                                   txn_root=pre_prepare.txnRootHash,
                                                   primaries=["Alpha", "Beta", "Gamma"])
    test_node.audit_handler.post_batch_applied(three_pc_batch)

    replica.order_3pc_key(key)

    ordered = replica.outBox.pop()
    assert ordered.primaries != test_node.primaries
    assert ordered.primaries == three_pc_batch.primaries


def test_primaries_in_ordered_from_node(test_node):
    pre_prepare = create_pre_prepare_no_bls(state_root=generate_state_root(), pp_seq_no=1)
    key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
    test_node.primaries = ["Alpha", "Beta"]
    replica = test_node.master_replica
    replica.prePrepares[key] = pre_prepare

    replica.order_3pc_key(key)

    ordered = replica.outBox.pop()
    assert ordered.primaries == test_node.primaries
