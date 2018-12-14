from datetime import datetime, timedelta
from random import shuffle

from copy import deepcopy

from plenum.common.metrics_collector import KvStoreMetricsCollector, MetricsName
from plenum.common.metrics_stats import trunc_ts, ValueAccumulator, MetricsStatsFrame, \
    MetricsStats, load_metrics_from_kv_store
from plenum.test.metrics.helper import generate_events
from plenum.test.helper import MockTimestamp


def _metrics_stats_frame(events):
    frame = MetricsStatsFrame()
    for ev in events:
        frame.add(ev.name, ev.value)
    return frame


def test_trunc_ts():
    base = datetime(year=2018, month=7, day=30, hour=13)

    assert trunc_ts(base.replace(minute=36, second=13, microsecond=300000), timedelta(seconds=1)) == \
           base.replace(minute=36, second=13)
    assert trunc_ts(base.replace(minute=36, second=13, microsecond=800000), timedelta(seconds=1)) == \
           base.replace(minute=36, second=13)

    assert trunc_ts(base.replace(minute=36, second=13), timedelta(seconds=10)) == base.replace(minute=36, second=10)
    assert trunc_ts(base.replace(minute=36, second=17), timedelta(seconds=10)) == base.replace(minute=36, second=10)

    assert trunc_ts(base.replace(minute=36, second=13), timedelta(seconds=20)) == base.replace(minute=36, second=0)
    assert trunc_ts(base.replace(minute=36, second=23), timedelta(seconds=20)) == base.replace(minute=36, second=20)

    assert trunc_ts(base.replace(minute=36, second=13), timedelta(minutes=5)) == base.replace(minute=35)
    assert trunc_ts(base.replace(minute=39, second=59), timedelta(minutes=5)) == base.replace(minute=35)
    assert trunc_ts(base.replace(minute=40, second=0), timedelta(minutes=5)) == base.replace(minute=40)


def test_metrics_stats_frame_can_add_values():
    events_transport_batch_size = [10, 2, 54]
    events_looper_run_time_spent = [0.1, 3.4, 0.01, 0.5]

    events = []
    for v in events_transport_batch_size:
        events.append((MetricsName.TRANSPORT_BATCH_SIZE, v))
    for v in events_looper_run_time_spent:
        events.append((MetricsName.LOOPER_RUN_TIME_SPENT, v))
    shuffle(events)

    frame = MetricsStatsFrame()
    for id, value in events:
        frame.add(id, value)

    assert frame.get(MetricsName.TRANSPORT_BATCH_SIZE) == ValueAccumulator(events_transport_batch_size)
    assert frame.get(MetricsName.LOOPER_RUN_TIME_SPENT) == ValueAccumulator(events_looper_run_time_spent)
    assert frame.get(MetricsName.BACKUP_THREE_PC_BATCH_SIZE) == ValueAccumulator()


def test_metrics_stats_frame_eq_has_value_semantics():
    a = MetricsStatsFrame()
    b = MetricsStatsFrame()
    assert a == b

    a.add(MetricsName.LOOPER_RUN_TIME_SPENT, 2.0)
    assert a != b

    b.add(MetricsName.LOOPER_RUN_TIME_SPENT, 2.0)
    assert a == b

    a.add(MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 1)
    b.add(MetricsName.TRANSPORT_BATCH_SIZE, 2)
    assert a != b


# TODO: This test should be split into several ones
def test_metrics_stats_can_add_values():
    stats = MetricsStats()

    min_ts = trunc_ts(datetime.utcnow(), stats.timestep)
    next_ts = min_ts + stats.timestep
    gap_ts = next_ts + 3 * stats.timestep
    max_ts = gap_ts + stats.timestep

    first_events = [ev for ev in generate_events(10, min_ts) if ev.timestamp < min_ts + stats.timestep]
    next_events = [ev for ev in generate_events(10, next_ts) if ev.timestamp < next_ts + stats.timestep]
    after_gap_events = [ev for ev in generate_events(10, gap_ts) if ev.timestamp < gap_ts + stats.timestep]

    all_events = first_events + next_events + after_gap_events
    for ev in all_events:
        stats.add(ev.timestamp, ev.name, ev.value)

    assert stats.min_ts == min_ts
    assert stats.max_ts == max_ts
    assert sorted(ts for ts, _ in stats.frames()) == [min_ts, next_ts, gap_ts]

    assert stats.frame(min_ts) == _metrics_stats_frame(first_events)
    assert stats.frame(next_ts) == _metrics_stats_frame(next_events)
    assert stats.frame(gap_ts) == _metrics_stats_frame(after_gap_events)
    assert stats.frame(max_ts) == MetricsStatsFrame()

    assert stats.frame(min_ts - 0.2 * stats.timestep) == MetricsStatsFrame()
    assert stats.frame(min_ts + 0.8 * stats.timestep) == _metrics_stats_frame(first_events)
    assert stats.frame(next_ts + 1.1 * stats.timestep) == MetricsStatsFrame()
    assert stats.frame(gap_ts - 0.1 * stats.timestep) == MetricsStatsFrame()
    assert stats.frame(max_ts + 4.2 * stats.timestep) == MetricsStatsFrame()


def test_metrics_stats_total_is_merge_of_all_frames():
    events = generate_events(50)

    stats = MetricsStats()
    for ev in events:
        stats.add(ev.timestamp, ev.name, ev.value)

    expected_total = MetricsStatsFrame()
    for _, frame in stats.frames():
        expected_total.merge(frame)

    assert stats.total == expected_total


def test_metrics_stats_merge_all_should_not_alter_source_frames():
    events = generate_events(50)

    stats = MetricsStats()
    for ev in events:
        stats.add(ev.timestamp, ev.name, ev.value)

    frames = [frame for _, frame in stats.frames()]
    saved_frames = deepcopy(frames)
    MetricsStats.merge_all(frames)

    assert frames == saved_frames


def test_metrics_stats_eq_has_value_semantics():
    ts = datetime.utcnow()

    a = MetricsStats()
    b = MetricsStats()
    assert a == b

    a.add(ts, MetricsName.LOOPER_RUN_TIME_SPENT, 2.0)
    assert a != b

    b.add(ts, MetricsName.LOOPER_RUN_TIME_SPENT, 2.0)
    assert a == b

    a.add(datetime.utcnow(), MetricsName.BACKUP_THREE_PC_BATCH_SIZE, 1)
    b.add(datetime.utcnow(), MetricsName.TRANSPORT_BATCH_SIZE, 2)
    assert a != b


def test_load_metrics_from_kv_store_can_load_all_values(storage):
    events = generate_events(10)
    step = timedelta(seconds=5)
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)
    expected_stats = MetricsStats(step)

    for ev in events:
        ts.value = ev.timestamp
        metrics.store_event(ev.name, ev.value)
        expected_stats.add(ev.timestamp, ev.name, ev.value)

    stats = load_metrics_from_kv_store(storage, step=step)
    assert stats == expected_stats


def test_load_metrics_from_kv_store_can_filter_values(storage):
    events = generate_events(10)
    step = timedelta(seconds=3)
    ts = MockTimestamp()
    metrics = KvStoreMetricsCollector(storage, ts)
    expected_stats = MetricsStats(step)

    timestamps = sorted(ev.timestamp for ev in events)
    min_ts = timestamps[len(events) // 3]
    max_ts = timestamps[2 * len(events) // 3]

    for ev in events:
        ts.value = ev.timestamp
        metrics.store_event(ev.name, ev.value)
        if min_ts <= ev.timestamp <= max_ts:
            expected_stats.add(ev.timestamp, ev.name, ev.value)

    stats = load_metrics_from_kv_store(storage, min_ts, max_ts, step)
    assert stats == expected_stats
