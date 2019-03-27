import pytest

from plenum.common.constants import CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID, POOL_LEDGER_ID, AUDIT_LEDGER_ID
from plenum.server.batch_handlers.audit_batch_handler import AuditBatchHandler

@pytest.fixture(scope="module")
def node(txnPoolNodeSet):
    return txnPoolNodeSet[0]

@pytest.fixture(scope="module")
def db_manager(node):
    return node.db_manager


@pytest.fixture(scope="function")
def initial_pool_size(node):
    return node.getLedger(POOL_LEDGER_ID).size


@pytest.fixture(scope="function")
def initial_domain_size(node):
    return node.getLedger(DOMAIN_LEDGER_ID).size


@pytest.fixture(scope="function")
def initial_config_size(node):
    return node.getLedger(CONFIG_LEDGER_ID).size


@pytest.fixture(scope="function")
def view_no(node):
    return node.master_replica.last_ordered_3pc[0]


@pytest.fixture(scope="function")
def pp_seq_no(node):
    return node.master_replica.last_ordered_3pc[1]


@pytest.fixture(scope="function")
def initial_seq_no(node):
    return node.getLedger(AUDIT_LEDGER_ID).size


@pytest.fixture(scope="function")
def alh(db_manager):
    audit_ledger_handler = AuditBatchHandler(db_manager)
    yield audit_ledger_handler
    for db in db_manager.databases.values():
        db.reset()
