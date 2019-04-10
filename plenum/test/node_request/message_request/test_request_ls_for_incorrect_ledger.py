from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.conftest import test_node
from plenum.test.logging.conftest import logsearch


def test_request_ls_for_incorrect_ledger(test_node, logsearch):
    incorrect_ledger_id = 12345
    correct_ledger_id = DOMAIN_LEDGER_ID
    log_msg_count, _ = logsearch(msgs=["Invalid ledger type: "
                                       "{}".format(incorrect_ledger_id)])
    old_re_ask_count = len(log_msg_count)
    assert not test_node.getLedgerStatus(incorrect_ledger_id)
    # check logs
    assert len(log_msg_count) - old_re_ask_count == 1
    # check that node build a ledger status for a correct ledger
    assert test_node.getLedgerStatus(correct_ledger_id)
