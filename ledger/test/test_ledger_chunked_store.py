from binascii import hexlify

import itertools
import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger
from ledger.serializers.json_serializer import JsonSerializer
from ledger.stores.chunked_file_store import ChunkedFileStore
from ledger.stores.file_hash_store import FileHashStore
from ledger.test.helper import check_ledger_generator
from ledger.test.test_file_hash_store import generateHashes

chunk_size = 5


@pytest.fixture(scope="function")
def ledger(tempdir):
    store = ChunkedFileStore(tempdir,
                             'transactions',
                             isLineNoKey=True,
                             chunkSize=chunk_size,
                             storeContentHash=False,
                             ensureDurability=False)
    ledger = Ledger(CompactMerkleTree(hashStore=FileHashStore(dataDir=tempdir)),
                    dataDir=tempdir, serializer=JsonSerializer(),
                    transactionLogStore=store)
    ledger.reset()
    return ledger


def test_add_get_txns(tempdir, ledger):
    txns = []
    hashes = [hexlify(h).decode() for h in generateHashes(60)]
    for i in range(20):
        txns.append({
            'a': hashes.pop(),
            'b': hashes.pop(),
            'c': hashes.pop()
        })

    for txn in txns:
        ledger.add(txn)

    check_ledger_generator(ledger)

    for s, t in ledger.getAllTxn(frm=1, to=20):
        assert txns[s-1] == t

    for s, t in ledger.getAllTxn(frm=3, to=8):
        assert txns[s-1] == t

    for s, t in ledger.getAllTxn(frm=5, to=17):
        assert txns[s-1] == t

    for s, t in ledger.getAllTxn(frm=6, to=10):
        assert txns[s-1] == t

    for s, t in ledger.getAllTxn(frm=3, to=3):
        assert txns[s-1] == t

    for s, t in ledger.getAllTxn(frm=3):
        assert txns[s-1] == t

    for s, t in ledger.getAllTxn(to=10):
        assert txns[s-1] == t

    for s, t in ledger.getAllTxn():
        assert txns[s-1] == t

    with pytest.raises(AssertionError):
        list(ledger.getAllTxn(frm=3, to=1))

    for frm, to in [(i, j) for i, j in itertools.permutations(range(1, 21),
                                                              2) if i <= j]:
        for s, t in ledger.getAllTxn(frm=frm, to=to):
            assert txns[s-1] == t
