import pytest
import base58

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from plenum.common.constants import TXN_TYPE, TARGET_NYM, DATA, NAME, ALIAS, SERVICES, VALIDATOR, IDENTIFIER
from plenum.common.stack_manager import TxnStackManager
from json.decoder import JSONDecodeError

whitelist = ['substring not found']

@pytest.fixture(scope="module")
def tdirWithLedger(tdir):
    tree = CompactMerkleTree()
    ledger = Ledger(CompactMerkleTree(), dataDir=tdir)
    for d in range(3):
        txn = { TXN_TYPE: '0',
                TARGET_NYM: base58.b58encode(b'whatever'),
                IDENTIFIER: "Th7MpTaRZVRYnPiabds81Y",
                DATA: {
                NAME: str(d),
                ALIAS: 'test' + str(d),
                SERVICES: [VALIDATOR],
                }
              }
        if d == 1:
            txn[TARGET_NYM] = "invalid===="
        ledger.add(txn)
    return ledger

@pytest.fixture(scope="module")
def invalid_identifier(tdir):
    tree = CompactMerkleTree()
    ledger = Ledger(CompactMerkleTree(), dataDir=tdir)
    txn = { TXN_TYPE: '0',
            TARGET_NYM: base58.b58encode(b'whatever'),
            IDENTIFIER: "invalid====",
            DATA: {
            NAME: str(2),
            ALIAS: 'test' + str(2),
            SERVICES: [VALIDATOR],
            }
          }
    ledger.add(txn)
    return ledger


class DummyLedger(Ledger):
    def getAllTxn(self, frm: int=None, to: int=None):
        raise JSONDecodeError('', '', 0)

                                                            
"""
Test that invalid base58 TARGET_NYM in pool_transaction raises the proper exception (INDY-150)
"""

def test_parse_verkey_non_base58_txn_type_field_raises_descriptive_error(tdirWithLedger,tdir):
    with pytest.raises(SystemExit) as excinfo:
        ledger = Ledger(CompactMerkleTree(), dataDir=tdir)
        _, _, nodeKeys = TxnStackManager.parseLedgerForHaAndKeys(ledger)
    assert excinfo.value.code == -1
    ledger.stop()


def test_parse_identifier_non_base58_txn_type_field_raises_descriptive_error(invalid_identifier, tdir):
    with pytest.raises(SystemExit) as excinfo:
        ledger = Ledger(CompactMerkleTree(), dataDir=tdir)
        _, _, nodeKeys = TxnStackManager.parseLedgerForHaAndKeys(ledger)
    assert excinfo.value.code == -1
    ledger.stop()


def test_pool_file_is_invalid_json_format_raises_JSONDecodeError(tdir):
    ledger = DummyLedger(CompactMerkleTree(), dataDir=tdir)
    with pytest.raises(SystemExit) as excinfo:
        _, _, nodeKeys = TxnStackManager.parseLedgerForHaAndKeys(ledger)
    assert excinfo.value.code == -1
    ledger.stop()

