from plenum.common.messages.internal_messages import NeedViewChange
from plenum.server.consensus.primary_selector import RoundRobinPrimariesSelector


def trigger_view_change_on_node(node, proposed_view_no):
    for r in node.replicas.values():
        r.internal_bus.send(NeedViewChange(proposed_view_no))
        if r.isMaster:
            assert r._consensus_data.waiting_for_new_view


def trigger_view_change(txnPoolNodeSet, proposed_view_no):
    for node in txnPoolNodeSet:
        trigger_view_change_on_node(node, proposed_view_no)


def get_next_primary_name(txnPoolNodeSet, expected_view_no):
    selector = RoundRobinPrimariesSelector()
    inst_count = len(txnPoolNodeSet[0].replicas)
    next_p_name = selector.select_primaries(expected_view_no, inst_count, txnPoolNodeSet[0].poolManager.node_names_ordered_by_rank())[0]
    return next_p_name
