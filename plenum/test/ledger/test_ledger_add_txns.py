from ledger.util import F
from plenum.common.txn_util import get_seq_no
from plenum.test.ledger.conftest import create_txns


def test_append_seq_no(ledger,
                       looper, sdk_wallet_client):
    txns = create_txns(looper, sdk_wallet_client)
    seq_no = 10
    txns = ledger._append_seq_no(txns, seq_no)
    for txn in txns:
        seq_no += 1
        assert get_seq_no(txn) == seq_no


def test_append_seq_no_when_adding(ledger,
                                   looper, sdk_wallet_client):
    txns = create_txns(looper, sdk_wallet_client)
    seq_no = ledger.seqNo
    for txn in txns:
        seq_no += 1
        assert get_seq_no(txn) is None
        ledger.add(txn)
        assert get_seq_no(txn) == seq_no


def test_add_result(ledger,
                    looper, sdk_wallet_client):
    txn = create_txns(looper, sdk_wallet_client)[0]
    res = ledger.add(txn)
    assert F.seqNo.name not in res
    assert F.auditPath.name in res
    assert F.rootHash.name in res
