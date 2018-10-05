import operator

from plenum.test.helper import sdk_send_batches_of_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data


def nodes_by_rank(txnPoolNodeSet):
    return [t[1] for t in sorted([(node.rank, node)
                                  for node in txnPoolNodeSet],
                                 key=operator.itemgetter(0))]


def sdk_ensure_pool_functional(looper, nodes, sdk_wallet, sdk_pool,
                               num_reqs=10, num_batches=2):
    sdk_send_batches_of_random_and_check(looper,
                                         nodes,
                                         sdk_pool,
                                         sdk_wallet,
                                         num_reqs,
                                         num_batches)
    ensure_all_nodes_have_same_data(looper, nodes)


def get_node_by_name(txnPoolNodeSet, name):
    return next(node for node in txnPoolNodeSet if node.name == name)


def nodes_last_ordered_equal(*nodes):
    if len(nodes) < 2:
        raise BaseException('nodes_last_ordered_equal can compare less than 2 nodes')
    seq_no = next(iter(nodes)).master_last_ordered_3PC[1]
    assert all(seq_no == n.master_last_ordered_3PC[1] for n in nodes)
