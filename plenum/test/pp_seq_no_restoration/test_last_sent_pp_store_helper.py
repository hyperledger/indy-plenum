from json import JSONDecodeError
from typing import Dict

import pytest

from common.serializers.serialization import node_status_db_serializer
from plenum.common.constants import LAST_SENT_PRE_PREPARE
from plenum.common.util import getNoInstances
from plenum.test.test_node import ensureElectionsDone, getPrimaryReplica
from plenum.test.view_change.helper import ensure_view_change

nodeCount = 7


def pack_pp_key(value: Dict) -> bytes:
    return node_status_db_serializer.serialize(value)


def unpack_pp_key(value: bytes) -> Dict:
    return node_status_db_serializer.deserialize(value)


@pytest.fixture(scope="module")
def view_no_set(looper, txnPoolNodeSet):
    for _ in range(2):
        ensure_view_change(looper, txnPoolNodeSet)
        ensureElectionsDone(looper, txnPoolNodeSet)
    assert txnPoolNodeSet[0].viewNo == 2


@pytest.fixture(scope="function")
def setup(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        if LAST_SENT_PRE_PREPARE in node.nodeStatusDB:
            node.nodeStatusDB.remove(LAST_SENT_PRE_PREPARE)
        for replica in node.replicas.values():
            replica.h = 0
            replica._lastPrePrepareSeqNo = 0
            replica.last_ordered_3pc = (replica.viewNo, 0)


@pytest.fixture(scope="function")
def replica_with_unknown_primary_status(txnPoolNodeSet, setup):
    replica = txnPoolNodeSet[0].replicas[1]
    old_primary_name = replica._primaryName
    replica._primaryName = None

    yield replica

    replica._primaryName = old_primary_name


def test_store_last_sent_pp_seq_no_if_some_stored(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          pack_pp_key({1: [2, 5]}))

    node.last_sent_pp_store_helper.store_last_sent_pp_seq_no(inst_id=1,
                                                             pp_seq_no=6)

    assert unpack_pp_key(node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE)) == \
           {'1': [2, 6]}


def test_store_last_sent_pp_seq_no_if_none_stored(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]

    node.last_sent_pp_store_helper.store_last_sent_pp_seq_no(inst_id=1,
                                                             pp_seq_no=6)

    assert unpack_pp_key(node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE)) == \
           {'1': [2, 6]}


def test_erase_last_sent_pp_seq_no_if_some_stored(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          pack_pp_key({'1': [2, 5]}))

    node.last_sent_pp_store_helper.erase_last_sent_pp_seq_no()

    assert LAST_SENT_PRE_PREPARE not in node.nodeStatusDB


def test_erase_last_sent_pp_seq_no_if_none_stored(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]

    node.last_sent_pp_store_helper.erase_last_sent_pp_seq_no()

    assert LAST_SENT_PRE_PREPARE not in node.nodeStatusDB


def test_try_restore_last_sent_pp_seq_no_if_relevant_stored(
        tconf, txnPoolNodeSet, view_no_set, setup):
    replica = getPrimaryReplica(txnPoolNodeSet, instId=1)
    node = replica.node
    assert node.viewNo == 2
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          pack_pp_key({1: [2, 5]}))

    node.last_sent_pp_store_helper.try_restore_last_sent_pp_seq_no()

    assert replica.lastPrePrepareSeqNo == 5
    assert replica.last_ordered_3pc == (2, 5)
    assert replica.h == 5
    assert replica.H == 5 + tconf.LOG_SIZE


def test_try_restore_last_sent_pp_seq_no_if_irrelevant_stored(
        tconf, txnPoolNodeSet, view_no_set, setup):
    replica = getPrimaryReplica(txnPoolNodeSet, instId=1)
    node = replica.node
    assert node.viewNo == 2
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          pack_pp_key({2: [1, 9]}))

    node.last_sent_pp_store_helper.try_restore_last_sent_pp_seq_no()

    assert replica.lastPrePrepareSeqNo == 0
    assert replica.last_ordered_3pc == (2, 0)
    assert replica.h == 0
    assert replica.H == 0 + tconf.LOG_SIZE


def test_try_restore_last_sent_pp_seq_no_if_none_stored(
        tconf, txnPoolNodeSet, view_no_set, setup):
    replica = getPrimaryReplica(txnPoolNodeSet, instId=1)
    node = replica.node
    assert node.viewNo == 2

    node.last_sent_pp_store_helper.try_restore_last_sent_pp_seq_no()

    assert replica.lastPrePrepareSeqNo == 0
    assert replica.last_ordered_3pc == (2, 0)
    assert replica.h == 0
    assert replica.H == 0 + tconf.LOG_SIZE


def test_try_restore_last_sent_pp_seq_no_if_invalid_stored(
        tconf, txnPoolNodeSet, view_no_set, setup):
    replica = getPrimaryReplica(txnPoolNodeSet, instId=1)
    node = replica.node
    assert node.viewNo == 2
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          pack_pp_key({1: [2, 5]})[:-1])

    node.last_sent_pp_store_helper.try_restore_last_sent_pp_seq_no()

    assert replica.lastPrePrepareSeqNo == 0
    assert replica.last_ordered_3pc == (2, 0)
    assert replica.h == 0
    assert replica.H == 0 + tconf.LOG_SIZE


def test_cannot_restore_last_sent_pp_seq_no_if_another_view(
        txnPoolNodeSet, view_no_set, setup):
    replica = getPrimaryReplica(txnPoolNodeSet, instId=1)
    node = replica.node
    assert node.viewNo == 2

    can = node.last_sent_pp_store_helper._can_restore_last_sent_pp_seq_no(
        1, [1, 5])

    assert can is False


def test_cannot_restore_last_sent_pp_seq_no_if_replica_absent(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    assert node.viewNo == 2
    absent_replica_index = getNoInstances(nodeCount)

    can = node.last_sent_pp_store_helper._can_restore_last_sent_pp_seq_no(
        absent_replica_index, [2, 5])

    assert can is False


def test_cannot_restore_last_sent_pp_seq_no_if_replica_status_unknown(
        view_no_set, setup, replica_with_unknown_primary_status):
    replica = replica_with_unknown_primary_status
    assert replica.instId == 1
    node = replica.node
    assert node.viewNo == 2

    can = node.last_sent_pp_store_helper._can_restore_last_sent_pp_seq_no(
        1, [2, 5])

    assert can is False


def test_cannot_restore_last_sent_pp_seq_no_if_replica_is_master(
        txnPoolNodeSet, view_no_set, setup):
    replica = getPrimaryReplica(txnPoolNodeSet, instId=0)
    node = replica.node
    assert node.viewNo == 2

    can = node.last_sent_pp_store_helper._can_restore_last_sent_pp_seq_no(
        0, [2, 5])

    assert can is False


def test_can_restore_last_sent_pp_seq_no_if_relevant(
        txnPoolNodeSet, view_no_set, setup):
    replica = getPrimaryReplica(txnPoolNodeSet, instId=1)
    node = replica.node
    assert node.viewNo == 2

    can = node.last_sent_pp_store_helper._can_restore_last_sent_pp_seq_no(
        1, [2, 5])

    assert can is True


def test_restore_last_sent_pp_seq_no(
        tconf, txnPoolNodeSet, view_no_set, setup):
    replica = getPrimaryReplica(txnPoolNodeSet, instId=1)
    node = replica.node
    assert node.viewNo == 2

    node.last_sent_pp_store_helper._restore_last_stored(
        1, [2, 5])

    for replica in node.replicas.values():
        if replica.instId == 1:
            assert replica.lastPrePrepareSeqNo == 5
            assert replica.last_ordered_3pc == (2, 5)
            assert replica.h == 5
            assert replica.H == 5 + tconf.LOG_SIZE
        else:
            assert replica.lastPrePrepareSeqNo == 0
            assert replica.last_ordered_3pc == (2, 0)
            assert replica.h == 0
            assert replica.H == tconf.LOG_SIZE


def test_can_load_absent_last_sent_pre_preapre_key(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]

    pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()

    assert pp_key is None


def test_cannot_load_last_sent_pre_preapre_key_if_empty_value(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, b'')

    with pytest.raises(JSONDecodeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_cannot_load_last_sent_pre_preapre_key_if_not_valid_dict(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize({1: [2, 5]})[:-1])

    with pytest.raises(JSONDecodeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_cannot_load_last_sent_pre_preapre_key_if_none(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize(None))

    with pytest.raises(TypeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_cannot_load_last_sent_pre_preapre_key_if_dict_has_no_entries(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize({}))

    with pytest.raises(TypeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_cannot_load_last_sent_pre_preapre_key_if_inst_id_missed(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize([2, 5]))

    with pytest.raises(TypeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_cannot_load_last_sent_pre_preapre_key_if_view_no_missed(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize([1, 5]))

    with pytest.raises(TypeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_cannot_load_last_sent_pre_preapre_key_if_pp_seq_no_missed(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize([1, 2]))

    with pytest.raises(TypeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_cannot_load_last_sent_pre_preapre_key_if_json_has_extra_fields(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize({'1': [2, 5, 1]}))

    with pytest.raises(TypeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_cannot_load_last_sent_pre_preapre_key_if_inst_id_is_not_int(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize({None: [2, 5]}))

    with pytest.raises(TypeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_cannot_load_last_sent_pre_preapre_key_if_view_no_is_not_int(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize({1: ['', 5]}))

    with pytest.raises(TypeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_cannot_load_last_sent_pre_preapre_key_if_pp_seq_not_int(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize({'1': [2, 5.0]}))

    with pytest.raises(TypeError):
        pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()


def test_can_load_valid_last_sent_pre_preapre_key_if_valid(
        txnPoolNodeSet, view_no_set, setup):
    node = txnPoolNodeSet[0]
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE,
                          node_status_db_serializer.serialize({'1': [2, 5]}))

    pp_key = node.last_sent_pp_store_helper._load_last_sent_pp_key()

    assert pp_key == {'1': [2, 5]}
