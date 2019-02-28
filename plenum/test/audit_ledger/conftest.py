import pytest

from plenum.common.constants import CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID, POOL_LEDGER_ID, AUDIT_LEDGER_ID
from plenum.server.batch_handlers.audit_batch_handler import AuditBatchHandler


@pytest.fixture(scope="module")
def db_manager(txnPoolNodeSet):
    return txnPoolNodeSet[0].db_manager


@pytest.fixture(scope="function")
def initial_pool_size(txnPoolNodeSet):
    return txnPoolNodeSet[0].getLedger(POOL_LEDGER_ID).size


@pytest.fixture(scope="function")
def initial_domain_size(txnPoolNodeSet):
    return txnPoolNodeSet[0].getLedger(DOMAIN_LEDGER_ID).size


@pytest.fixture(scope="function")
def initial_config_size(txnPoolNodeSet):
    return txnPoolNodeSet[0].getLedger(CONFIG_LEDGER_ID).size


@pytest.fixture(scope="function")
def view_no(txnPoolNodeSet):
    return txnPoolNodeSet[0].master_replica.last_ordered_3pc[0]


@pytest.fixture(scope="function")
def pp_seq_no(txnPoolNodeSet):
    return txnPoolNodeSet[0].master_replica.last_ordered_3pc[1]


@pytest.fixture(scope="function")
def initial_seq_no(txnPoolNodeSet):
    return txnPoolNodeSet[0].getLedger(AUDIT_LEDGER_ID).size


@pytest.fixture(scope="function")
def alh(db_manager):
    audit_ledger_handler = AuditBatchHandler(db_manager)
    yield audit_ledger_handler
    for db in db_manager.databases.values():
        db.reset()
