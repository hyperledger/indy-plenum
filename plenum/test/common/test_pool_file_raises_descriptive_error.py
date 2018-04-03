import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from plenum.common.stack_manager import TxnStackManager
from json.decoder import JSONDecodeError

errMsg = 'Pool transaction file corrupted. Rebuild pool transactions.'
whitelist = [errMsg]


class DummyLedger(Ledger):
    def getAllTxn(self, frm: int = None, to: int = None):
        raise JSONDecodeError('', '', 0)


def test_pool_file_is_invalid_raises_SystemExit_has_descriptive_error(
        tdir_for_func):
    """
    Test that that invalid pool_transaction file raises the proper exception (INDY-150)
    """
    ledger = DummyLedger(CompactMerkleTree(), dataDir=tdir_for_func)
    with pytest.raises(SystemExit) as excinfo:
        _, _, nodeKeys = TxnStackManager.parseLedgerForHaAndKeys(ledger)
    assert excinfo.value.code == errMsg
    ledger.stop()
