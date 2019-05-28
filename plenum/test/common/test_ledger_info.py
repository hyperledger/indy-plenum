from plenum.common.ledger_info import LedgerInfo

from plenum.test.testing_utils import FakeSomething


def test_taa_acceptance_required_default():
    assert LedgerInfo(
        0, FakeSomething(),
        preCatchupStartClbk=None,
        postCatchupCompleteClbk=None,
        postTxnAddedToLedgerClbk=None,
        verifier=None
    ).taa_acceptance_required == True
