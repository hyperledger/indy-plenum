import pytest

from plenum.common.startable import Mode


def test_can_send_3pc_batch_by_primary_only(primary_orderer):
    assert primary_orderer.can_send_3pc_batch()
    primary_orderer.primary_name = "SomeNode:0"
    assert not primary_orderer.can_send_3pc_batch()


def test_can_send_3pc_batch_not_participating(primary_orderer, mode):
    primary_orderer._data.node_mode = mode
    result = primary_orderer.can_send_3pc_batch()
    assert result == (mode == Mode.participating)


# ToDO: uncomment this test, if pre_view_change_in_progress will be used
# def test_can_send_3pc_batch_pre_view_change(orderer, mode):
#     orderer.replica.node.pre_view_change_in_progress = True
#     primary_validator.replica.node.mode = mode
#     assert not primary_validator.can_send_3pc_batch()


def test_can_send_3pc_batch_old_view(primary_orderer, mode):
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no + 1, 0)
    primary_orderer._data.node_mode = mode
    assert not primary_orderer.can_send_3pc_batch()


def test_can_send_3pc_batch_old_pp_seq_no_for_view(primary_orderer, mode):
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, 100)
    primary_orderer._lastPrePrepareSeqNo = 0
    primary_orderer._data.node_mode = mode
    assert not primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('initial_seq_no', [0, 3, 8, 13])
def test_can_send_multiple_3pc_batches(primary_orderer, initial_seq_no, monkeypatch):
    monkeypatch.setattr(primary_orderer._config, 'Max3PCBatchesInFlight', None)
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, initial_seq_no)
    primary_orderer._lastPrePrepareSeqNo = initial_seq_no + 10
    assert primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('initial_seq_no', [0, 3, 8, 13])
@pytest.mark.parametrize('num_in_flight', [0, 1, 2, 3])
def test_can_send_multiple_3pc_batches_below_limit(primary_orderer, initial_seq_no, num_in_flight, monkeypatch):
    limit = 4
    monkeypatch.setattr(primary_orderer._config, 'Max3PCBatchesInFlight', limit)
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, initial_seq_no)
    primary_orderer._lastPrePrepareSeqNo = initial_seq_no + num_in_flight
    assert primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('initial_seq_no', [0, 3, 8, 13])
@pytest.mark.parametrize('above_limit', [0, 1, 2, 5, 10])
def test_cannot_send_multiple_3pc_batches_above_limit(primary_orderer, initial_seq_no, above_limit, monkeypatch):
    limit = 4
    monkeypatch.setattr(primary_orderer._config, 'Max3PCBatchesInFlight', limit)
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no, initial_seq_no)
    primary_orderer._lastPrePrepareSeqNo = initial_seq_no + limit + above_limit
    assert not primary_orderer.can_send_3pc_batch()


@pytest.mark.parametrize('initial_seq_no', [0, 3, 8, 13])
@pytest.mark.parametrize('num_in_flight', [0, 1, 2, 3, 4, 5, 10])
def test_can_send_multiple_3pc_batches_in_next_view(primary_orderer, initial_seq_no, num_in_flight, monkeypatch):
    limit = 4
    monkeypatch.setattr(primary_orderer._config, 'Max3PCBatchesInFlight', limit)
    primary_orderer.last_ordered_3pc = (primary_orderer.view_no - 1, initial_seq_no)
    primary_orderer._lastPrePrepareSeqNo = initial_seq_no + num_in_flight
    assert primary_orderer.can_send_3pc_batch()
