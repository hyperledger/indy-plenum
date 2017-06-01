from plenum.server.pool_manager import RegistryPoolManager, TxnPoolManager
from plenum.test.test_node import checkProtocolInstanceSetup


def check_rank_consistent_across_each_node(nodes):
    assert nodes
    node_ranks = {}
    name_by_ranks = {}
    for node in nodes:
        node_ranks[node.poolManager.id] = node.rank
        name_by_ranks[node.rank] = node.name

    for node in nodes:
        for other_node in nodes:
            if node != other_node:
                oid = other_node.poolManager.id
                assert node.poolManager.get_rank_of(oid) ==  node_ranks[oid]
                ork = node_ranks[oid]
                assert node.poolManager.get_name_by_rank(ork) ==  name_by_ranks[ork]
    order = []
    for node in nodes:
        if isinstance(node.poolManager, RegistryPoolManager):
            order.append(node.poolManager.node_names_ordered_by_rank)
        elif isinstance(node.poolManager, TxnPoolManager):
            order.append(node.poolManager.node_ids_in_ordered_by_rank)
        else:
            RuntimeError('Dont know this pool manager {}'.
                         format(node.poolManager))

    assert len(order) == len(nodes)
    assert order.count(order[0]) == len(order)  # All elements are same


def check_newly_added_node(looper, nodes, new_node):
    assert new_node in nodes
    check_rank_consistent_across_each_node(nodes)
    assert all(new_node.rank > n.rank for n in nodes[:-1])
    checkProtocolInstanceSetup(looper, nodes, retryWait=1)
