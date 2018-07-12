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


def test_non_Master_watermarks_not_maxsize_if_is_primary(replica):
    replica.isMaster = False
    replica._primaryName = replica.name
    replica.h = 100
    replica.on_propagate_primary_done()
    assert replica.H != sys.maxsize
