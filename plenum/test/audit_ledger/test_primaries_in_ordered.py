from plenum.common.messages.node_messages import Ordered
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root


def test_primaries_in_ordered_from_audit(test_node):
    pre_prepare = create_pre_prepare_no_bls(state_root=generate_state_root(), pp_seq_no=1)
    replica = test_node.master_replica
    key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
    replica.prePrepares[key] = pre_prepare
    test_node.primaries = ["Alpha", "Beta"]
    three_pc_batch = ThreePcBatch.from_pre_prepare(pre_prepare=pre_prepare,
                                                   state_root=pre_prepare.stateRootHash,
                                                   txn_root=pre_prepare.txnRootHash,
                                                   primaries=["Alpha", "Beta", "Gamma"],
                                                   valid_digests=pre_prepare.reqIdr)
    test_node.write_manager.audit_b_handler.post_batch_applied(three_pc_batch)

    replica.order_3pc_key(key)

    ordered = replica.outBox.pop()
    assert ordered.primaries != test_node.primaries
    assert ordered.primaries == three_pc_batch.primaries


def test_primaries_in_ordered_from_audit_for_tree_txns(test_node):
    primaries = {}
    replica = test_node.master_replica
    test_node.primaries = ["Alpha", "Beta"]
    for i in range(3):
        pp = create_pre_prepare_no_bls(state_root=generate_state_root(),
                                       pp_seq_no=i)
        key = (pp.viewNo, pp.ppSeqNo)
        replica.prePrepares[key] = pp
        three_pc_batch = ThreePcBatch.from_pre_prepare(pre_prepare=pp,
                                                       state_root=pp.stateRootHash,
                                                       txn_root=pp.txnRootHash,
                                                       primaries=["Node{}".format(num)
                                                                  for num in range(i + 1)],
                                                       valid_digests=pp.reqIdr)
        test_node.write_manager.audit_b_handler.post_batch_applied(three_pc_batch)
        primaries[key] = three_pc_batch.primaries

    for key in reversed(list(primaries.keys())):
        replica.order_3pc_key(key)

    for ordered in replica.outBox:
        if not isinstance(ordered, Ordered):
            continue
        assert ordered.primaries != test_node.primaries
        assert ordered.primaries == primaries[(ordered.viewNo,
                                               ordered.ppSeqNo)]


def test_primaries_in_ordered_from_node(test_node):
    pre_prepare = create_pre_prepare_no_bls(state_root=generate_state_root(), pp_seq_no=1)
    key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
    test_node.primaries = ["Alpha", "Beta"]
    replica = test_node.master_replica
    replica.prePrepares[key] = pre_prepare

    replica.order_3pc_key(key)

    ordered = replica.outBox.pop()
    assert ordered.primaries == test_node.primaries
