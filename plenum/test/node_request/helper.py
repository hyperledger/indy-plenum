from plenum.test.helper import sdk_send_batches_of_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data


def sdk_ensure_pool_functional(looper, nodes,
                           sdk_wallet, sdk_pool,
                           num_reqs=10, num_batches=2):
    sdk_send_batches_of_random_and_check(looper,
                                         nodes,
                                         sdk_pool,
                                         sdk_wallet,
                                         num_reqs,
                                         num_batches)
    ensure_all_nodes_have_same_data(looper, nodes)