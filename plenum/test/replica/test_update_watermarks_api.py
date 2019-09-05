import sys

import pytest

from plenum.test.replica.helper import emulate_catchup, emulate_select_primaries


def test_propagate_primary_is_Master_update_watermarks(replica):
    # expected behaviour is that h must be set as last ordered ppSeqNo
    replica.isMaster = True
    replica._checkpointer.set_watermarks(low_watermark=0)
    replica.last_ordered_3pc = (replica.viewNo, 500)
    assert replica.h == 0
    replica.on_propagate_primary_done()
    assert replica.h == 500


def test_propagate_primary_is_Master_watermarks_not_changed_if_last_ordered_not_changed(replica):
    replica.isMaster = True
    replica._checkpointer.set_watermarks(low_watermark=0)
    assert replica.h == 0
    replica.on_propagate_primary_done()
    assert replica.h == 0


def test_propagate_primary_not_Master_update_watermarks_to_maxsize(replica):
    # expected behaviour is that h must be set as last ordered ppSeqNo
    replica.isMaster = False
    replica._checkpointer.set_watermarks(low_watermark=0)
    replica.last_ordered_3pc = (0, 500)
    assert replica.h == 0
    replica.on_propagate_primary_done()
    assert replica.h == 0
    assert replica.H == sys.maxsize


def test_propagate_primary_not_Master_watermarks_not_changed_if_last_ordered_not_changed(replica):
    replica.isMaster = False
    replica._checkpointer.set_watermarks(low_watermark=0)
    assert replica.h == 0
    replica.on_propagate_primary_done()
    assert replica.h == 0


def test_propagate_primary_non_Master_watermarks_not_maxsize_if_is_primary(replica, tconf):
    replica.isMaster = False
    replica._consensus_data.primary_name = replica.name
    replica._checkpointer.set_watermarks(low_watermark=100)
    replica.on_propagate_primary_done()
    assert replica.H == 100 + tconf.LOG_SIZE


def test_catchup_clear_for_backup(replica):
    last_ordered_before = replica.last_ordered_3pc
    replica._consensus_data.primary_name = None
    replica.isMaster = False
    replica._catchup_clear_for_backup()
    assert replica.last_ordered_3pc == last_ordered_before
    assert replica.h == 0
    assert replica.H == sys.maxsize


def test_do_not_reset_watermarks_before_new_view_on_master(replica, tconf):
    replica.isMaster = True
    h_before = 100
    last_pp_before = replica._ordering_service._lastPrePrepareSeqNo
    replica._checkpointer.set_watermarks(low_watermark=h_before)
    assert replica.h == h_before
    assert replica.H == h_before + tconf.LOG_SIZE
    assert replica._ordering_service._lastPrePrepareSeqNo == last_pp_before


def test_catchup_without_vc_and_no_primary_on_master(replica, tconf):
    ppSeqNo = 100
    replica.isMaster = True
    replica._consensus_data.primary_name = None
    # next calls emulate a catchup without vc when there is no primary selected
    # like it will be called for 'update watermark' procedure
    emulate_catchup(replica, ppSeqNo)
    # select_primaries after allLedgersCaughtUp
    emulate_select_primaries(replica)
    assert replica.h == ppSeqNo
    assert replica.H == ppSeqNo + tconf.LOG_SIZE


def test_catchup_without_vc_and_no_primary_on_backup(replica, tconf):
    ppSeqNo = 100
    replica.isMaster = False
    replica._consensus_data.primary_name = None
    # next calls emulate a catchup without vc when there is no primary selected
    # like it will be called for 'update watermark' procedure
    emulate_catchup(replica, ppSeqNo)
    # select_primaries after allLedgersCaughtUp
    emulate_select_primaries(replica)
    assert replica.h == 0
    assert replica.H == sys.maxsize


def test_catchup_without_during_vc_with_primary_on_master(replica, tconf):
    # this test emulate situation of simple catchup procedure without view_change
    # (by checkpoints or ledger_statuses)
    ppSeqNo = 100
    replica._consensus_data.primary_name = 'SomeNode'
    replica.isMaster = True
    emulate_catchup(replica, ppSeqNo)
    assert replica.last_ordered_3pc == (replica.viewNo, 100)
    assert replica.h == ppSeqNo
    assert replica.H == ppSeqNo + tconf.LOG_SIZE


def test_catchup_without_during_vc_with_primary_on_backup(replica):
    # this test emulate situation of simple catchup procedure without view_change
    # (by checkpoints or ledger_statuses)
    caughtup_pp_seq_no = 100
    current_pp_seq_no = 50
    replica._ordering_service.last_ordered_3pc = (replica.viewNo, current_pp_seq_no)
    replica._ordering_service.first_batch_after_catchup = False
    replica._consensus_data.primary_name = 'SomeNode'
    replica._consensus_data._name = "SomeAnotherNode"
    replica.isMaster = False
    emulate_catchup(replica, caughtup_pp_seq_no)
    assert replica.last_ordered_3pc == (replica.viewNo, current_pp_seq_no)
    assert replica.h == 0
    assert replica.H == sys.maxsize
    assert replica._ordering_service.first_batch_after_catchup


def test_view_change_no_propagate_primary_on_master(replica, tconf):
    ppSeqNo = 100
    replica.isMaster = True
    replica._consensus_data.primary_name = 'SomeNode'
    # next calls emulate simple view_change procedure (replica's watermark related steps)
    emulate_catchup(replica, ppSeqNo)
    emulate_select_primaries(replica)
    assert replica.h == ppSeqNo
    assert replica.H == ppSeqNo + tconf.LOG_SIZE


def test_view_change_no_propagate_primary_on_backup(replica, tconf):
    ppSeqNo = 100
    replica.isMaster = False
    replica._consensus_data.primary_name = 'SomeNode'
    # next calls emulate simple view_change procedure (replica's watermark related steps)
    emulate_catchup(replica, ppSeqNo)
    emulate_select_primaries(replica)
    assert replica.h == 0
    assert replica.H == sys.maxsize


def test_view_change_propagate_primary_on_master(replica, tconf):
    ppSeqNo = 100
    replica.isMaster = True
    replica._consensus_data.primary_name = 'SomeNode'
    # next calls emulate view_change for propagate primary situation
    # (when the new node join to the pool)
    emulate_catchup(replica, ppSeqNo)
    emulate_select_primaries(replica)
    replica.on_propagate_primary_done()
    assert replica.h == ppSeqNo
    assert replica.H == ppSeqNo + tconf.LOG_SIZE


def test_view_change_propagate_primary_on_backup(replica):
    ppSeqNo = 100
    replica.isMaster = False
    replica._consensus_data.primary_name = 'SomeNode'
    # next calls emulate view_change for propagate primary situation
    # (when the new node join to the pool)
    emulate_catchup(replica, ppSeqNo)
    emulate_select_primaries(replica)
    replica.on_propagate_primary_done()
    assert replica.h == 0
    assert replica.H == sys.maxsize
