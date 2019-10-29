from plenum.server.consensus.primary_selector import RoundRobinPrimariesSelector


def get_next_primary_name(txnPoolNodeSet, expected_view_no):
    selector = RoundRobinPrimariesSelector()
    inst_count = len(txnPoolNodeSet[0].replicas)
    next_p_name = selector.select_primaries(expected_view_no, inst_count, txnPoolNodeSet[0].poolManager.node_names_ordered_by_rank())[0]
    return next_p_name


def trigger_view_change(nodes):
    for node in nodes:
        node.view_changer.on_master_degradation()
