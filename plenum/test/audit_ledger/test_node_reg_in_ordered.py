from plenum.common.messages.node_messages import Ordered
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.consensus.utils import preprepare_to_batch_id
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root


def test_node_reg_in_ordered_from_audit(test_node):
    pre_prepare = create_pre_prepare_no_bls(state_root=generate_state_root(), pp_seq_no=1)
    replica = test_node.master_replica
    key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
    replica._ordering_service.prePrepares[key] = pre_prepare
    replica._consensus_data.preprepared.append(preprepare_to_batch_id(pre_prepare))
    test_node.primaries = ["Alpha", "Beta"]
    three_pc_batch = ThreePcBatch.from_pre_prepare(pre_prepare=pre_prepare,
                                                   state_root=pre_prepare.stateRootHash,
                                                   txn_root=pre_prepare.txnRootHash,
                                                   primaries=["Alpha", "Beta", "Gamma"],
                                                   valid_digests=pre_prepare.reqIdr)
    three_pc_batch.node_reg = ["Alpha", "Beta", "Gamma", "Delta", "Eta"]
    test_node.write_manager.audit_b_handler.post_batch_applied(three_pc_batch)

    replica._ordering_service._order_3pc_key(key)

    ordered = replica.outBox.pop()
    assert ordered.nodeReg == three_pc_batch.node_reg


def test_node_reg_in_ordered_from_audit_for_tree_txns(test_node):
    node_regs = {}
    replica = test_node.master_replica
    test_node.primaries = ["Alpha", "Beta"]
    node_reg = ["Alpha", "Beta", "Gamma", "Delta", "Eta"]
    for i in range(3):
        pp = create_pre_prepare_no_bls(state_root=generate_state_root(),
                                       pp_seq_no=i)
        key = (pp.viewNo, pp.ppSeqNo)
        replica._ordering_service.prePrepares[key] = pp
        replica._consensus_data.preprepared.append(preprepare_to_batch_id(pp))
        three_pc_batch = ThreePcBatch.from_pre_prepare(pre_prepare=pp,
                                                       state_root=pp.stateRootHash,
                                                       txn_root=pp.txnRootHash,
                                                       primaries=["Node{}".format(num)
                                                                  for num in range(i + 1)],
                                                       valid_digests=pp.reqIdr)
        three_pc_batch.node_reg = node_reg + ["Node{}".format(i + 10)]
        test_node.write_manager.audit_b_handler.post_batch_applied(three_pc_batch)
        node_regs[key] = three_pc_batch.node_reg

    for key in reversed(list(node_regs.keys())):
        replica._ordering_service._order_3pc_key(key)

    for ordered in replica.outBox:
        if not isinstance(ordered, Ordered):
            continue
        assert ordered.nodeReg == node_regs[(ordered.viewNo,
                                             ordered.ppSeqNo)]


def test_node_reg_in_ordered_from_audit_no_node_reg(test_node):
    replica = test_node.master_replica

    # run multiple times to have a situation when we write node reg to non-empty audit ledger
    for i in range(1, 4):
        uncommitted_node_reg = ['Alpha', 'Beta', 'Gamma', 'Node{}'.format(i)]
        test_node.write_manager.node_reg_handler.uncommitted_node_reg = uncommitted_node_reg

        pre_prepare = create_pre_prepare_no_bls(state_root=generate_state_root(), pp_seq_no=i)
        key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
        replica._ordering_service.prePrepares[key] = pre_prepare
        replica._consensus_data.preprepared.append(preprepare_to_batch_id(pre_prepare))
        test_node.primaries = ["Alpha", "Beta"]
        three_pc_batch = ThreePcBatch.from_pre_prepare(pre_prepare=pre_prepare,
                                                       state_root=pre_prepare.stateRootHash,
                                                       txn_root=pre_prepare.txnRootHash,
                                                       primaries=["Alpha", "Beta", "Gamma"],
                                                       valid_digests=pre_prepare.reqIdr)
        three_pc_batch.node_reg = None
        test_node.write_manager.audit_b_handler.post_batch_applied(three_pc_batch)

        replica._ordering_service._order_3pc_key(key)

        ordered = replica.outBox.pop()
        # we expect it equal to the current uncommitted node reg
        assert ordered.nodeReg == uncommitted_node_reg
