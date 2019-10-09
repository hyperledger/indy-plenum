from plenum.common.util import compare_3PC_keys
from plenum.test.test_node import get_master_primary_node, getNonPrimaryReplicas


def count_msg_reqs_of_type(node, typ):
    return sum([1 for entry in node.spylog.getAll(node.process_message_req)
                if entry.params['msg'].msg_type == typ])


def count_msg_reps_of_type(node, typ):
    return sum([1 for entry in node.spylog.getAll(node.process_message_rep)
                if entry.params['msg'].msg_type == typ])


def split_nodes(nodes):
    primary_node = get_master_primary_node(nodes)
    slow_node = getNonPrimaryReplicas(nodes, 0)[-1].node
    other_nodes = [n for n in nodes if n != slow_node]
    other_non_primary_nodes = [n for n in nodes if n not in
                               (slow_node, primary_node)]
    return slow_node, other_nodes, primary_node, other_non_primary_nodes


def check_pp_out_of_sync(alive_nodes, disconnected_nodes, last_ordered_key):
    def get_last_pp_key(node):
        last_pp = node.master_replica._ordering_service.last_preprepare
        if last_pp is None:
            return node.master_replica._ordering_service.last_ordered_3pc
        return last_pp.viewNo, last_pp.ppSeqNo

    for node in alive_nodes:
        assert compare_3PC_keys(last_ordered_key, get_last_pp_key(node)) > 0

    for node in disconnected_nodes:
        assert get_last_pp_key(node) == last_ordered_key
