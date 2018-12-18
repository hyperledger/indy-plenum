import os

import pytest

from plenum.persistence.req_id_to_txn import ReqIdrToTxn
from storage.helper import initKeyValueStorage


@pytest.fixture(scope="module")
def req_ids_to_txn(tconf):
    dataLocation = tconf.GENERAL_CONFIG_DIR + "/req_id_to_txn"
    if not os.path.isdir(dataLocation):
        os.makedirs(dataLocation)
    return ReqIdrToTxn(
        initKeyValueStorage(
            tconf.reqIdToTxnStorage,
            dataLocation,
            tconf.seqNoDbName)
    )


def test_req_id_to_txn_add(req_ids_to_txn):
    digest = "random_req_digest"
    ledger_id = 1
    seq_no = 123
    req_ids_to_txn.add(digest, ledger_id, seq_no)
    new_ledger_id, new_seq_no = req_ids_to_txn.get(digest)
    assert new_ledger_id == ledger_id
    assert seq_no == seq_no


def test_req_id_to_txn_add_batch(req_ids_to_txn):
    batch = [("random_req_digest" + str(index),
              1,
              123 + index)
             for index in range(3)]
    req_ids_to_txn.addBatch(batch)
    for digest, ledge_id, seq_no in batch:
        new_ledger_id, new_seq_no = req_ids_to_txn.get(digest)
        assert new_ledger_id == ledge_id
        assert new_seq_no == seq_no
