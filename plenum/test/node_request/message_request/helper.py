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
