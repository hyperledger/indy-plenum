import pytest

from plenum.common.util import SortedDict
from plenum.common.messages.node_messages import PrePrepare, Commit

# from plenum.test.replica.conftest import *
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.replica_helper import generateName
from plenum.test.consensus.order_service.conftest import primary_orderer as _primary_orderer
from plenum.test.helper import MockTimestamp, create_prepare, generate_state_root, create_commit_no_bls_sig
from plenum.test.testing_utils import FakeSomething


OBSOLETE_PP_TS = 0


class FakeSomethingHashable(FakeSomething):
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(tuple(SortedDict(self.__dict__).items()))


class FakeMessageBase(FakeSomethingHashable):
    _fields = {}


class FakePrePrepare(FakeMessageBase, PrePrepare):
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(tuple(SortedDict(self.__dict__).items()))


@pytest.fixture(scope='module')
def sender():
    return 'some_replica'


@pytest.fixture(scope='module')
def ts_now(tconf):
    return OBSOLETE_PP_TS + tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS + 1


@pytest.fixture
def viewNo():
    return 0


@pytest.fixture
def inst_id():
    return 0


@pytest.fixture
def mock_timestamp():
    return MockTimestamp(OBSOLETE_PP_TS)


@pytest.fixture
def primary_orderer(_primary_orderer, ts_now, mock_timestamp):
    _primary_orderer.last_accepted_pre_prepare_time = None
    _primary_orderer.get_time_for_3pc_batch = mock_timestamp
    _primary_orderer.get_time_for_3pc_batch.value = ts_now
    return _primary_orderer


@pytest.fixture
def sender_orderer(primary_orderer, sender, inst_id):
    return generateName(sender, inst_id)


@pytest.fixture
def pp(primary_orderer, ts_now, inst_id):
    return FakePrePrepare(
        instId=inst_id,
        viewNo=primary_orderer.view_no,
        ppSeqNo=(primary_orderer.last_ordered_3pc[1] + 1),
        ppTime=ts_now,
        reqIdr=tuple(),
        digest=OrderingService.generate_pp_digest([], primary_orderer.view_no, ts_now),
        auditTxnRootHash="HSai3sMHKeAva4gWMabDrm1yNhezvPHfXnGyHf2ex1L4"
    )


def test_pp_obsolete_if_older_than_last_accepted(primary_orderer, ts_now, sender, pp, sender_orderer):
    primary_orderer.last_accepted_pre_prepare_time = ts_now
    pp = FakeSomethingHashable(viewNo=0, ppSeqNo=1, ppTime=OBSOLETE_PP_TS)

    primary_orderer.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp, sender_orderer] = \
        primary_orderer.last_accepted_pre_prepare_time

    assert not primary_orderer._is_pre_prepare_time_correct(pp, sender)


def test_pp_obsolete_if_unknown(primary_orderer, pp):
    pp = FakeSomethingHashable(viewNo=0, ppSeqNo=1, ppTime=OBSOLETE_PP_TS, auditTxnRootHash="abcdef")
    assert not primary_orderer._is_pre_prepare_time_correct(pp, '')


def test_pp_obsolete_if_older_than_threshold(primary_orderer, ts_now, pp, sender_orderer):
    pp = FakeSomethingHashable(viewNo=0, ppSeqNo=1, ppTime=OBSOLETE_PP_TS, auditTxnRootHash="abcdef")

    primary_orderer.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp.auditTxnRootHash, sender_orderer] = ts_now

    assert not primary_orderer._is_pre_prepare_time_correct(pp, sender_orderer)


def test_ts_is_set_for_obsolete_pp(primary_orderer, ts_now, sender, pp, sender_orderer):
    pp.ppTime = OBSOLETE_PP_TS
    primary_orderer.process_preprepare(pp, sender_orderer)
    assert primary_orderer.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp.auditTxnRootHash, sender_orderer] == ts_now


def test_ts_is_set_for_passed_pp(primary_orderer, ts_now, sender, pp, sender_orderer):
    primary_orderer.process_preprepare(pp, sender_orderer)
    assert primary_orderer.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp.auditTxnRootHash, sender_orderer] == ts_now


def test_ts_is_set_for_discarded_pp(primary_orderer, ts_now, sender, pp, sender_orderer):
    pp.instId += 1
    primary_orderer.process_preprepare(pp, sender_orderer)
    assert primary_orderer.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp.auditTxnRootHash, sender_orderer] == ts_now


def test_ts_is_set_for_stahed_pp(primary_orderer, ts_now, sender, pp, sender_orderer):
    pp.viewNo += 1
    primary_orderer.process_preprepare(pp, sender_orderer)
    assert primary_orderer.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp.auditTxnRootHash, sender_orderer] == ts_now


def test_ts_is_not_set_for_non_pp(primary_orderer, ts_now, sender, pp, sender_orderer):
    primary_orderer.process_prepare(create_prepare(req_key=(0, 1), state_root=generate_state_root()), sender_orderer)
    primary_orderer.process_commit(create_commit_no_bls_sig(req_key=(0, 1)), sender_orderer)
    assert len(primary_orderer.pre_prepare_tss) == 0


def test_pre_prepare_tss_is_cleaned_in_gc(primary_orderer, pp, sender_orderer):
    primary_orderer.process_preprepare(pp, sender_orderer)

    # threshold is lower
    primary_orderer.gc((pp.viewNo, pp.ppSeqNo - 1))
    assert (pp.viewNo, pp.ppSeqNo) in primary_orderer.pre_prepare_tss

    # threshold is not lower
    primary_orderer.gc((pp.viewNo, pp.ppSeqNo))
    assert (pp.viewNo, pp.ppSeqNo) not in primary_orderer.pre_prepare_tss
