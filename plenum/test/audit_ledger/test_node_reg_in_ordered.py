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
    three_pc_batch = ThreePcBatch.from_pre_prepare(pre_prepare=pre_prepare,
                                                   state_root=pre_prepare.stateRootHash,
                                                   txn_root=pre_prepare.txnRootHash,
                                                   valid_digests=pre_prepare.reqIdr)
    three_pc_batch.node_reg = ["Alpha", "Beta", "Gamma", "Delta", "Eta"]
    three_pc_batch.primaries = ["Alpha", "Beta"]
    test_node.write_manager.audit_b_handler.post_batch_applied(three_pc_batch)

    assert replica._ordering_service._get_node_reg_for_ordered(pre_prepare) == three_pc_batch.node_reg


def test_node_reg_in_ordered_from_audit_for_tree_txns(test_node):
    node_regs = {}
    replica = test_node.master_replica
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
                                                       valid_digests=pp.reqIdr)
        three_pc_batch.node_reg = node_reg + ["Node{}".format(i + 10)]
        three_pc_batch.primaries = ["Alpha", "Beta"]
        test_node.write_manager.audit_b_handler.post_batch_applied(three_pc_batch)
        node_regs[key] = three_pc_batch.node_reg

    for key in reversed(list(node_regs.keys())):
        pp = replica._ordering_service.get_preprepare(*key)
        assert replica._ordering_service._get_node_reg_for_ordered(pp) == node_regs[key]
