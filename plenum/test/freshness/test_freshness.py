import pytest

from plenum.test.freshness.helper import get_all_multi_sig_values_for_all_nodes, \
    check_updated_bls_multi_sig_for_all_ledgers, check_freshness_updated_for_all
from plenum.test.helper import freshness
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_update_bls_multi_sig_by_timeout(looper, tconf, txnPoolNodeSet):
    # 1. Wait for the first freshness update
    looper.run(eventually(
        check_freshness_updated_for_all, txnPoolNodeSet,
        timeout=FRESHNESS_TIMEOUT + 5)
    )

    # 2. Wait for the second freshness update
    bls_multi_sigs_after_first_update = get_all_multi_sig_values_for_all_nodes(txnPoolNodeSet)
    looper.run(eventually(check_updated_bls_multi_sig_for_all_ledgers,
                          txnPoolNodeSet, bls_multi_sigs_after_first_update, FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))
