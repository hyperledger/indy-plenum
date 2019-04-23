import pytest

from plenum.common.util import SortedDict
from plenum.common.messages.node_messages import PrePrepare

from plenum.test.replica.conftest import *
from plenum.test.replica.conftest import primary_replica as _primary_replica
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
def primary_replica(_primary_replica, ts_now):
    _primary_replica.last_accepted_pre_prepare_time = None
    _primary_replica.get_time_for_3pc_batch.value = ts_now
    _primary_replica.threePhaseRouter.add((PrePrepare, lambda *x, **y: None))
    return _primary_replica


@pytest.fixture
def sender_replica(primary_replica, sender):
    return primary_replica.generateName(sender, primary_replica.instId)


@pytest.fixture
def pp(primary_replica, ts_now):
    return FakePrePrepare(
        instId=primary_replica.instId,
        viewNo=primary_replica.viewNo,
        ppSeqNo=(primary_replica.last_ordered_3pc[1] + 1),
        ppTime=ts_now,
        reqIdr=tuple()
    )


def test_pp_obsolete_if_older_than_last_accepted(primary_replica, ts_now, sender, pp, sender_replica):
    primary_replica.last_accepted_pre_prepare_time = ts_now
    pp = FakeSomethingHashable(viewNo=0, ppSeqNo=1, ppTime=OBSOLETE_PP_TS)

    primary_replica.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp, sender_replica] = \
        primary_replica.last_accepted_pre_prepare_time

    assert not primary_replica.is_pre_prepare_time_correct(pp, sender)


def test_pp_obsolete_if_unknown(primary_replica, pp):
    pp = FakeSomethingHashable(viewNo=0, ppSeqNo=1, ppTime=OBSOLETE_PP_TS)
    assert not primary_replica.is_pre_prepare_time_correct(pp, '')


def test_pp_obsolete_if_older_than_threshold(primary_replica, ts_now, pp, sender_replica):
    pp = FakeSomethingHashable(viewNo=0, ppSeqNo=1, ppTime=OBSOLETE_PP_TS)

    primary_replica.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp, sender_replica] = ts_now

    assert not primary_replica.is_pre_prepare_time_correct(pp, sender_replica)


def test_ts_is_set_for_obsolete_pp(primary_replica, ts_now, sender, pp, sender_replica):
    pp.ppTime = OBSOLETE_PP_TS
    primary_replica.process_three_phase_msg(pp, sender)
    assert primary_replica.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp, sender_replica] == ts_now


def test_ts_is_set_for_passed_pp(primary_replica, ts_now, sender, pp, sender_replica):
    primary_replica.process_three_phase_msg(pp, sender)
    assert primary_replica.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp, sender_replica] == ts_now


def test_ts_is_set_for_discarded_pp(primary_replica, ts_now, sender, pp, sender_replica):
    pp.instId +=1
    primary_replica.process_three_phase_msg(pp, sender)
    assert primary_replica.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp, sender_replica] == ts_now


def test_ts_is_set_for_stahed_pp(primary_replica, ts_now, sender, pp, sender_replica):
    pp.viewNo +=1
    primary_replica.process_three_phase_msg(pp, sender)
    assert primary_replica.pre_prepare_tss[pp.viewNo, pp.ppSeqNo][pp, sender_replica] == ts_now


def test_ts_is_not_set_for_non_pp(primary_replica, ts_now, sender, pp, sender_replica):
    pp = FakeSomethingHashable(**pp.__dict__)
    primary_replica.threePhaseRouter.add((FakeSomethingHashable, lambda *x, **y: None))
    primary_replica.process_three_phase_msg(pp, sender)
    assert len(primary_replica.pre_prepare_tss) == 0


def test_pre_prepare_tss_is_cleaned_in_gc(primary_replica, pp, sender):
    primary_replica.process_three_phase_msg(pp, sender)

    # threshold is lower
    primary_replica._gc((pp.viewNo, pp.ppSeqNo - 1))
    assert (pp.viewNo, pp.ppSeqNo) in primary_replica.pre_prepare_tss

    # threshold is not lower
    primary_replica._gc((pp.viewNo, pp.ppSeqNo))
    assert (pp.viewNo, pp.ppSeqNo) not in primary_replica.pre_prepare_tss
