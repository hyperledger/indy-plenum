import pytest

from plenum.server.monitor import RequestTimeTracker

INSTANCE_COUNT = 4


@pytest.fixture(scope="function")
def req_tracker():
    instances = set(range(INSTANCE_COUNT))
    removed_replica = INSTANCE_COUNT // 2
    instances.remove(removed_replica)
    return RequestTimeTracker(instances)


def test_request_tracker_start_adds_request(req_tracker):
    digest = "digest"
    now = 1.0

    req_tracker.start(digest, now)

    assert digest in req_tracker
    assert req_tracker.started(digest) == now
    assert digest in req_tracker.unordered()
    assert digest in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest not in req_tracker.handled_unordered()


def test_request_tracker_handle_makes_request_handled_unordered(req_tracker):
    digest = "digest"
    now = 1.0

    req_tracker.start(digest, now)
    req_tracker.handle(digest)

    assert digest in req_tracker
    assert digest in req_tracker.unordered()
    assert digest not in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest in req_tracker.handled_unordered()


def test_request_tracker_reset_clears_all_requests(req_tracker):
    digest = "digest"
    now = 1.0

    req_tracker.start(digest, now)
    req_tracker.handle(digest)
    req_tracker.reset()

    assert digest not in req_tracker
    assert digest not in req_tracker.unordered()
    assert digest not in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest not in req_tracker.handled_unordered()


def test_request_tracker_order_by_master_makes_request_ordered_and_returns_time_to_order(req_tracker):
    digest = "digest"
    now = 1.0
    req_tracker.start(digest, now)

    tto = req_tracker.order(0, digest, now + 5)

    assert digest not in req_tracker.unordered()
    assert digest not in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest not in req_tracker.handled_unordered()
    assert int(tto) == 5


def test_request_tracker_order_by_master_makes_handled_request_ordered_and_returns_time_to_order(req_tracker):
    digest = "digest"
    now = 1.0
    req_tracker.start(digest, now)
    req_tracker.handle(digest)

    tto = req_tracker.order(0, digest, now + 5)

    assert digest not in req_tracker.unordered()
    assert digest not in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest not in req_tracker.handled_unordered()
    assert int(tto) == 5


def test_request_tracker_order_by_backup_returns_time_to_order(req_tracker):
    digest = "digest"
    now = 1.0
    req_tracker.start(digest, now)

    tto = req_tracker.order(1, digest, now + 5)

    assert digest in req_tracker.unordered()
    assert digest in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest not in req_tracker.handled_unordered()
    assert int(tto) == 5


def test_request_tracker_deletes_request_only_when_it_is_ordered_by_all_instances(req_tracker):
    digest = "digest"
    now = 1.0
    req_tracker.start(digest, now)

    for instId in range(INSTANCE_COUNT - 1):
        req_tracker.order(instId, digest, now)
        assert digest in req_tracker

    req_tracker.order(INSTANCE_COUNT - 1, digest, now)
    assert digest not in req_tracker
    assert digest not in req_tracker.unordered()
    assert digest not in req_tracker.handled_unordered()


def test_request_tracker_doesnt_wait_for_new_instances_on_old_requests(req_tracker):
    digest = "digest"
    now = 1.0

    req_tracker.start(digest, now)
    req_tracker.add_instance(INSTANCE_COUNT)

    for instId in range(INSTANCE_COUNT):
        req_tracker.order(instId, digest, now)

    assert digest not in req_tracker
    assert digest not in req_tracker.unordered()
    assert digest not in req_tracker.handled_unordered()


def test_request_tracker_waits_for_new_instances_on_new_requests(req_tracker):
    digest = "digest"
    now = 1.0

    req_tracker.add_instance(INSTANCE_COUNT)
    req_tracker.start(digest, now)

    for instId in range(INSTANCE_COUNT):
        req_tracker.order(instId, digest, now)
    assert digest in req_tracker

    req_tracker.order(INSTANCE_COUNT, digest, now)
    assert digest not in req_tracker
    assert digest not in req_tracker.unordered()
    assert digest not in req_tracker.handled_unordered()


def test_request_tracker_performs_garbage_collection_on_remove_instance(req_tracker):
    digest = "digest"
    now = 1.0
    req_tracker.start(digest, now)

    req_tracker.order(1, digest, now)
    req_tracker.order(2, digest, now)

    req_tracker.remove_instance(0)
    assert digest in req_tracker

    req_tracker.remove_instance(3)
    assert digest not in req_tracker
    assert digest not in req_tracker.unordered()
    assert digest not in req_tracker.handled_unordered()


def test_force_req_drop_not_started(req_tracker):
    digest = "digest"

    req_tracker.force_req_drop(digest)


def test_force_req_drop_started(req_tracker):
    digest = "digest"
    now = 1.0
    req_tracker.start(digest, now)

    assert digest in req_tracker
    assert digest in req_tracker.unordered()
    assert digest in [digest for digest, _ in req_tracker.unhandled_unordered()]

    req_tracker.force_req_drop(digest)

    assert digest not in req_tracker
    assert digest not in req_tracker.unordered()
    assert digest not in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest not in req_tracker.handled_unordered()


def test_force_req_drop_handled(req_tracker):
    digest = "digest"
    now = 1.0

    req_tracker.start(digest, now)
    req_tracker.handle(digest)

    assert digest in req_tracker
    assert digest in req_tracker.unordered()
    assert digest not in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest in req_tracker.handled_unordered()

    req_tracker.force_req_drop(digest)

    assert digest not in req_tracker
    assert digest not in req_tracker.unordered()
    assert digest not in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest not in req_tracker.handled_unordered()


def test_force_req_drop_between_ordered_master(req_tracker):
    digest = "digest"
    start_ts = 1.0
    now = 3.0

    req_tracker.start(digest, start_ts)

    tto = req_tracker.order(0, digest, now)
    assert tto == 2.0

    assert digest not in req_tracker.unordered()

    req_tracker.force_req_drop(digest)

    assert digest not in req_tracker
    assert digest not in req_tracker.unordered()
    assert digest not in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest not in req_tracker.handled_unordered()

    tto = req_tracker.order(1, digest, now)
    assert tto == 0.0


def test_force_req_drop_between_ordered_backup(req_tracker):
    digest = "digest"
    start_ts = 1.0
    now = 3.0

    req_tracker.start(digest, start_ts)

    tto = req_tracker.order(1, digest, now)
    assert tto == 2.0

    assert digest in req_tracker.unordered()

    req_tracker.force_req_drop(digest)

    assert digest not in req_tracker
    assert digest not in req_tracker.unordered()
    assert digest not in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest not in req_tracker.handled_unordered()

    tto = req_tracker.order(2, digest, now)
    assert tto == 0.0


def test_force_req_drop_before_handle(req_tracker):
    digest = "digest"
    now = 1.0

    req_tracker.start(digest, now)

    req_tracker.force_req_drop(digest)

    assert digest not in req_tracker
    assert digest not in req_tracker.unordered()
    assert digest not in [digest for digest, _ in req_tracker.unhandled_unordered()]
    assert digest not in req_tracker.handled_unordered()

    req_tracker.handle(digest)
    assert digest not in req_tracker.handled_unordered()
