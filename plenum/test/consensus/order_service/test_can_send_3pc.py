import pytest

from plenum.common.startable import Mode


def test_can_send_3pc_batch_by_primary_only(primary_orderer):
    assert primary_orderer.can_send_3pc_batch()
    primary_orderer._data.primary_name = "SomeNode:0"
    assert not primary_orderer.can_send_3pc_batch()


def test_can_send_3pc_batch_not_participating(primary_orderer, mode):
    primary_orderer._data.node_mode = mode
    result = primary_orderer.can_send_3pc_batch()
    assert result == (mode == Mode.participating)


def test_can_send_3pc_batch_old_view(primary_orderer, mode):
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no + 1, 0)
    primary_orderer._data.node_mode = mode
    assert not primary_orderer.can_send_3pc_batch()


def test_can_send_3pc_batch_old_pp_seq_no_for_view(primary_orderer, mode):
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, 100)
    primary_orderer._lastPrePrepareSeqNo = 0
    primary_orderer._data.node_mode = mode
    assert not primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('initial_seq_no', [1, 3, 8, 13])
def test_can_send_multiple_3pc_batches(primary_orderer, initial_seq_no, monkeypatch):
    monkeypatch.setattr(primary_orderer._config, 'Max3PCBatchesInFlight', None)
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, initial_seq_no)
    primary_orderer._lastPrePrepareSeqNo = initial_seq_no + 10
    assert primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('initial_seq_no', [1, 3, 8, 13])
@pytest.mark.parametrize('num_in_flight', [0, 1, 2, 3])
def test_can_send_multiple_3pc_batches_below_limit(primary_orderer, initial_seq_no, num_in_flight, monkeypatch):
    limit = 4
    monkeypatch.setattr(primary_orderer._config, 'Max3PCBatchesInFlight', limit)
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, initial_seq_no)
    primary_orderer._lastPrePrepareSeqNo = initial_seq_no + num_in_flight
    assert primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('initial_seq_no', [1, 3, 8, 13])
@pytest.mark.parametrize('above_limit', [0, 1, 2, 5, 10])
def test_cannot_send_multiple_3pc_batches_above_limit(primary_orderer, initial_seq_no, above_limit, monkeypatch):
    limit = 4
    monkeypatch.setattr(primary_orderer._config, 'Max3PCBatchesInFlight', limit)
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, initial_seq_no)
    primary_orderer._lastPrePrepareSeqNo = initial_seq_no + limit + above_limit
    assert not primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('initial_seq_no', [1, 3, 8, 13])
@pytest.mark.parametrize('num_in_flight', [0, 1, 2, 3, 4, 5, 10])
def test_can_send_multiple_3pc_batches_in_next_view(primary_orderer, initial_seq_no, num_in_flight, monkeypatch):
    limit = 4
    monkeypatch.setattr(primary_orderer._config, 'Max3PCBatchesInFlight', limit)
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no - 1, initial_seq_no)
    primary_orderer._lastPrePrepareSeqNo = initial_seq_no + num_in_flight
    assert primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('last_pp_seqno', [0, 1, 9])
def test_cannot_send_3pc_batch_below_prev_view_prep_cert(primary_orderer, last_pp_seqno):
    primary_orderer._data.prev_view_prepare_cert = 10
    primary_orderer._lastPrePrepareSeqNo = last_pp_seqno
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, last_pp_seqno)
    assert not primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('last_pp_seqno', [0, 9, 10])
def test_can_send_3pc_batch_None_prev_view_prep_cert(primary_orderer, last_pp_seqno):
    primary_orderer._data.prev_view_prepare_cert = 0
    primary_orderer._lastPrePrepareSeqNo = last_pp_seqno
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, last_pp_seqno)
    assert primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('last_pp_seqno', [10, 11, 100])
def test_can_send_3pc_batch_above_prev_view_prep_cert(primary_orderer, last_pp_seqno):
    primary_orderer._data.prev_view_prepare_cert = 10
    primary_orderer._lastPrePrepareSeqNo = last_pp_seqno
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, last_pp_seqno)
    assert primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('last_ordered_3pc, can_send',
                         [
                             ((0, 0), False),
                             ((0, 1), False),
                             ((0, 9), False),
                             ((0, 10), False),
                             ((0, 11), True),
                             ((0, 12), True),
                             ((0, 13), True)
                         ])
def test_can_not_send_3pc_until_first_batch_in_non_zero_view_ordered(primary_orderer, last_ordered_3pc, can_send):
    primary_orderer._data.view_no = 1
    primary_orderer._data.prev_view_prepare_cert = 10
    primary_orderer._lastPrePrepareSeqNo = max(11, last_ordered_3pc[1])
    primary_orderer._data.last_ordered_3pc = last_ordered_3pc
    assert primary_orderer.can_send_3pc_batch() == can_send


@pytest.mark.parametrize('last_ordered_3pc, can_send',
                         [
                             ((0, 0), True),
                             ((0, 1), True),
                             ((0, 9), True),
                             ((0, 10), True),
                             ((0, 11), True),
                             ((0, 12), True),
                             ((0, 13), True)
                         ])
def test_can_send_3pc_before_first_batch_in_zero_view_ordered(primary_orderer, last_ordered_3pc, can_send, monkeypatch):
    monkeypatch.setattr(primary_orderer._config, 'Max3PCBatchesInFlight', 20)
    primary_orderer._data.view_no = 0
    primary_orderer._data.prev_view_prepare_cert = 10
    primary_orderer._lastPrePrepareSeqNo = max(11, last_ordered_3pc[1])
    primary_orderer._data.last_ordered_3pc = last_ordered_3pc
    assert primary_orderer.can_send_3pc_batch() == can_send
