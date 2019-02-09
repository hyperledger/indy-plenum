import pytest

from plenum.test.bls.helper import sdk_change_bls_key
from plenum.test.freshness.helper import check_freshness_updated_for_all
from plenum.test.helper import sdk_send_random_and_check, freshness
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 3


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def check_audit_ledger_updated(audit_size_initial, nodes):
    audit_size_after = [node.auditLedger.size for node in nodes]
    assert all(audit_size_after[i] == audit_size_initial[i] + 1 for i in range(len(nodes)))


def test_audit_ledger_updated_after_freshness_updated(looper, tconf, txnPoolNodeSet):
    audit_size_initial = [node.auditLedger.size for node in txnPoolNodeSet]

    # Wait for a freshness update
    looper.run(eventually(
        check_freshness_updated_for_all, txnPoolNodeSet,
        timeout=2 * FRESHNESS_TIMEOUT)
    )

    check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet)


def test_audit_ledger_updated_after_domain_txns(looper, txnPoolNodeSet,
                                                sdk_pool_handle, sdk_wallet_client):
    audit_size_initial = [node.auditLedger.size for node in txnPoolNodeSet]
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet)


def test_audit_ledger_updated_after_pool_txns(looper, txnPoolNodeSet,
                                              sdk_pool_handle, sdk_wallet_stewards):
    audit_size_initial = [node.auditLedger.size for node in txnPoolNodeSet]
    sdk_change_bls_key(looper, txnPoolNodeSet,
                       txnPoolNodeSet[3],
                       sdk_pool_handle,
                       sdk_wallet_stewards[3],
                       check_functional=False)
    check_audit_ledger_updated(audit_size_initial, txnPoolNodeSet)


def test_audit_ledger_format(txnPoolNodeSet):
    pass
