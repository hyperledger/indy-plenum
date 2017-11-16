from typing import Sequence, List

from plenum.server.pool_manager import RegistryPoolManager, TxnPoolManager
from plenum.test.test_node import TestNode, checkProtocolInstanceSetup


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
                assert node.poolManager.get_rank_of(oid) == node_ranks[oid]
                ork = node_ranks[oid]
                assert node.poolManager.get_name_by_rank(
                    ork) == name_by_ranks[ork]
    order = []
    for node in nodes:
        if isinstance(node.poolManager, RegistryPoolManager):
            order.append(node.poolManager.node_names_ordered_by_rank())
        elif isinstance(node.poolManager, TxnPoolManager):
            order.append(node.poolManager.node_ids_ordered_by_rank())
        else:
            RuntimeError('Dont know this pool manager {}'.
                         format(node.poolManager))

    assert len(order) == len(nodes), "order {} nodes {}".format(order, nodes)
    # All elements are same
    assert order.count(order[0]) == len(order), \
        "order count {}, order {}".format(order.count(order[0]), order)


def check_newly_added_nodes(looper, all_nodes, new_nodes):
    # New nodes should be give in the order they were added
    assert [n in all_nodes for n in new_nodes]
    check_rank_consistent_across_each_node(all_nodes)
    old_nodes = [node for node in all_nodes if node not in new_nodes]
    for new_node in new_nodes:
        assert all(new_node.rank > n.rank for n in old_nodes)
        old_nodes.append(new_node)
    checkProtocolInstanceSetup(looper, all_nodes, retryWait=1)


def getPrimaryNodesIdxs(nodes: Sequence[TestNode]) -> List[TestNode]:
    primariesIdxs = []
    for instId in range(len(nodes[0].replicas)):
        for idx, node in enumerate(nodes):
            if node.replicas[instId].isPrimary:
                assert instId == len(primariesIdxs)
                primariesIdxs.append(idx)

    assert len(set(primariesIdxs)) == len(nodes[0].replicas)
    return primariesIdxs
