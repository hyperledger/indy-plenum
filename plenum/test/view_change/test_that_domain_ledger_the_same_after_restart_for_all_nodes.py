import pytest

from plenum.test.view_change.helper import ensure_view_change_by_primary_restart
from plenum.test.pool_transactions.conftest import looper
from stp_core.common.log import getlogger
from plenum.common.startable import Mode


logger = getlogger()


@pytest.fixture(scope="function", autouse=True)
def limitTestRunningTime():
    return 600


def catchuped(node):
    assert node.mode == Mode.participating


def test_that_domain_ledger_the_same_after_restart_for_all_nodes(
                looper, txnPoolNodeSet, tdir, tconf,
                allPluginsPath, limitTestRunningTime):

    def prepare_for_compare(domain_ledger):
        dict_for_compare = {}
        dict_for_compare['hashes'] = domain_ledger.tree.hashes
        dict_for_compare['root_hash'] = domain_ledger.root_hash
        dict_for_compare['tree_root_hash'] = domain_ledger.tree.root_hash
        dict_for_compare['tree_root_hash_hex'] = domain_ledger.tree.root_hash_hex

        c_pos = domain_ledger.tree.hashStore.leavesFile.db_file.tell()
        domain_ledger.tree.hashStore.leavesFile.db_file.seek(0, 0)
        dict_for_compare['leaves_store'] = domain_ledger.tree.hashStore.leavesFile.db_file.read()
        domain_ledger.tree.hashStore.leavesFile.db_file.seek(c_pos)

        c_pos = domain_ledger.tree.hashStore.nodesFile.db_file.tell()
        domain_ledger.tree.hashStore.nodesFile.db_file.seek(0, 0)
        dict_for_compare['nodes_store'] = domain_ledger.tree.hashStore.nodesFile.db_file.read()
        domain_ledger.tree.hashStore.nodesFile.db_file.seek(c_pos)

        dict_for_compare['txns'] = [(tno, txn) for tno, txn in domain_ledger.getAllTxn()]

        return dict_for_compare

    def compare(before, after):
        for k,v in before.items():
            if k in after:
                if v != after[k]:
                    logger.debug("compare_ledgers: before[{}]!=after[{}]".format(k, k))
                    logger.debug("compare_ledgers: before value: {}".format(v))
                    logger.debug("compare_ledgers: after value: {}".format(after[k]))
                    for k, v in before.items():
                        logger.debug("compare_ledgers: before : {}: {}".format(k, v))
                        logger.debug("compare_ledgers: after_dict: {}: {}".format(k, after.get(k)))
                    assert False


    pool_of_nodes = txnPoolNodeSet
    for __ in range(4):
        p_node = [node for node in pool_of_nodes if node.has_master_primary][0]
        before_vc_dict = prepare_for_compare(p_node.domainLedger)
        pool_of_nodes = ensure_view_change_by_primary_restart(looper,
                                                                pool_of_nodes,
                                                                tconf,
                                                                tdir,
                                                                allPluginsPath,
                                                                customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
        for node in pool_of_nodes:
            logger.debug("primary node before view_change: {}, compared node: {}".format(p_node, node))
            after_vc_dict = prepare_for_compare(node.domainLedger)
            compare(before_vc_dict, after_vc_dict)