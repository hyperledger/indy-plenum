import sys


def test_is_Master_update_watermarks(replica):
    # expected behaviour is that h must be set as last ordered ppSeqNo
    replica.isMaster = True
    replica.h = 0
    replica.last_ordered_3pc = (0, 500)
    assert replica.h == 0
    replica.on_propagate_primary_done()
    assert replica.h == 500


def test_is_Master_watermarks_not_changed_if_last_ordered_not_changed(replica):
    replica.isMaster = True
    replica.h = 0
    assert replica.h == 0
    replica.on_propagate_primary_done()
    assert replica.h == 0


def test_not_Master_update_watermarks_to_maxsize(replica):
    # expected behaviour is that h must be set as last ordered ppSeqNo
    replica.isMaster = False
    replica.h = 0
    replica.last_ordered_3pc = (0, 500)
    assert replica.h == 0
    replica.on_propagate_primary_done()
    assert replica.h == 0
    assert replica.H == sys.maxsize


def test_not_Master_watermarks_not_changed_if_last_ordered_not_changed(replica):
    replica.isMaster = False
    replica.h = 0
    assert replica.h == 0
    replica.on_propagate_primary_done()
    assert replica.h == 0


def test_non_Master_watermarks_not_maxsize_if_is_primary(replica, tconf):
    replica.isMaster = False
    replica._primaryName = replica.name
    replica.h = 100
    replica.on_propagate_primary_done()
    assert replica.H == 100 + tconf.LOG_SIZE


def test_catchup_clear_for_backup(replica):
    replica.node.viewNo = 1
    replica._primaryName = None
    replica.isMaster = False
    replica.catchup_clear_for_backup()
    assert replica.last_ordered_3pc == (replica.viewNo, 0)
    assert replica.h == 0
    assert replica.H == sys.maxsize

def test_reset_watermarks_before_new_view(replica, tconf):
    replica.h = 100
    replica._reset_watermarks_before_new_view()
    assert replica.h == 0
    assert replica.H == tconf.LOG_SIZE
    assert replica._lastPrePrepareSeqNo == replica.h


def test_catchup_without_vc_and_no_primary(replica, tconf):
    replica.node.viewNo = 10
    replica.isMaster = False
    replica._primaryName = None
    replica.catchup_clear_for_backup()
    replica._reset_watermarks_before_new_view()
    replica._setup_for_non_master_after_view_change(replica.viewNo)
    assert replica.h == 0
    assert replica.H == tconf.LOG_SIZE


def test_catchup_without_during_vc_with_primary(replica):
    replica.node.viewNo = 10
    replica._primaryName = 'SomeNode'
    replica.isMaster = False
    replica.catchup_clear_for_backup()
    assert replica.last_ordered_3pc == (replica.viewNo, 0)
    assert replica.h == 0
    assert replica.H == sys.maxsize


def test_view_change_no_propagate_primary(replica, tconf):
    replica.node.viewNo = 10
    replica.isMaster = False
    replica._primaryName = 'SomeNode'
    replica.catchup_clear_for_backup()
    replica._reset_watermarks_before_new_view()
    replica._setup_for_non_master_after_view_change(replica.viewNo)
    assert replica.h == 0
    assert replica.H == tconf.LOG_SIZE


def test_view_change_propagate_primary_on_master(replica, tconf):
    replica.node.viewNo = 10
    replica.isMaster = True
    replica._primaryName = 'SomeNode'
    replica.catchup_clear_for_backup()
    replica._reset_watermarks_before_new_view()
    replica._setup_for_non_master_after_view_change(replica.viewNo)
    replica.on_propagate_primary_done()
    assert replica.h == 0
    assert replica.H == tconf.LOG_SIZE


def test_view_change_propagate_primary_on_backup(replica):
    replica.node.viewNo = 10
    replica.isMaster = False
    replica._primaryName = 'SomeNode'
    replica.catchup_clear_for_backup()
    replica._reset_watermarks_before_new_view()
    replica._setup_for_non_master_after_view_change(replica.viewNo)
    replica.on_propagate_primary_done()
    assert replica.h == 0
    assert replica.H == sys.maxsize
