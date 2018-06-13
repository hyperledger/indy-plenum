
from plenum.server.monitor import RequestTimeTracker

INSTANCE_COUNT = 4


def test_request_tracker_start_adds_request():
    req_tracker = RequestTimeTracker(INSTANCE_COUNT)
    digest = "digest"
    now = 1.0

    req_tracker.start(digest, now)

    assert digest in req_tracker
    assert digest in [req for req, _ in req_tracker.unordered()]


def test_request_tracker_order_by_master_makes_request_ordered_and_returns_time_to_order():
    req_tracker = RequestTimeTracker(INSTANCE_COUNT)
    digest = "digest"
    now = 1.0
    req_tracker.start(digest, now)

    tto = req_tracker.order(0, digest, now + 5)

    assert digest not in [digest for digest, _ in req_tracker.unordered()]
    assert int(tto) == 5


def test_request_tracker_order_by_backup_returns_time_to_order():
    req_tracker = RequestTimeTracker(INSTANCE_COUNT)
    digest = "digest"
    now = 1.0
    req_tracker.start(digest, now)

    tto = req_tracker.order(1, digest, now + 5)

    assert digest in [digest for digest, _ in req_tracker.unordered()]
    assert int(tto) == 5


def test_request_tracker_deletes_request_only_when_it_is_ordered_by_all_instances():
    req_tracker = RequestTimeTracker(INSTANCE_COUNT)
    digest = "digest"
    now = 1.0
    req_tracker.start(digest, now)

    for instId in range(INSTANCE_COUNT - 1):
        req_tracker.order(instId, digest, now)
        assert digest in req_tracker

    req_tracker.order(INSTANCE_COUNT - 1, digest, now)
    assert digest not in req_tracker


def test_request_tracker_doesnt_wait_for_new_instances_on_old_requests():
    req_tracker = RequestTimeTracker(INSTANCE_COUNT)
    digest = "digest"
    now = 1.0

    req_tracker.start(digest, now)
    req_tracker.add_instance()

    for instId in range(INSTANCE_COUNT):
        req_tracker.order(instId, digest, now)

    assert digest not in req_tracker


def test_request_tracker_waits_for_new_instances_on_new_requests():
    req_tracker = RequestTimeTracker(INSTANCE_COUNT)
    digest = "digest"
    now = 1.0

    req_tracker.add_instance()
    req_tracker.start(digest, now)

    for instId in range(INSTANCE_COUNT):
        req_tracker.order(instId, digest, now)
    assert digest in req_tracker

    req_tracker.order(INSTANCE_COUNT, digest, now)
    assert digest not in req_tracker


def test_request_tracker_performs_garbage_collection_on_remove_instance():
    req_tracker = RequestTimeTracker(INSTANCE_COUNT)
    digest = "digest"
    now = 1.0
    req_tracker.start(digest, now)

    req_tracker.order(1, digest, now)
    req_tracker.order(2, digest, now)

    req_tracker.remove_instance(0)
    assert digest in req_tracker

    req_tracker.remove_instance(2)
    assert digest not in req_tracker
