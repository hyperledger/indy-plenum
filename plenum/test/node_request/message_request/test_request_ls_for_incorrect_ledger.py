from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.conftest import vdr_test_node


def test_request_ls_for_incorrect_ledger(vdr_test_node):
    incorrect_ledger_id = 12345
    correct_ledger_id = DOMAIN_LEDGER_ID

    assert not vdr_test_node.getLedgerStatus(incorrect_ledger_id)
    # check that node build a ledger status for a correct ledger
    assert vdr_test_node.getLedgerStatus(correct_ledger_id)
