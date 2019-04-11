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
    payload_digest = 'random_payload'
    req_ids_to_txn.add(payload_digest, ledger_id, seq_no, digest)
    new_ledger_id, new_seq_no = req_ids_to_txn.get_by_payload_digest(payload_digest)
    assert new_ledger_id == ledger_id
    assert seq_no == seq_no

    assert req_ids_to_txn.get_by_full_digest(digest) == payload_digest


def test_req_id_to_txn_add_batch(req_ids_to_txn):
    batch = [('random_payload' + str(index),
              1,
              123 + index,
              "random_req_digest" + str(index))
             for index in range(3)]
    req_ids_to_txn.addBatch(batch)
    for payload_digest, ledger_id, seq_no, digest in batch:
        new_ledger_id, new_seq_no = req_ids_to_txn.get_by_payload_digest(payload_digest)
        assert new_ledger_id == ledger_id
        assert new_seq_no == seq_no

        assert req_ids_to_txn.get_by_full_digest(digest) == payload_digest
