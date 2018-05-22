import random
import time
from collections import OrderedDict

from plenum.common.util import randomString

try:
    import ujson as json
except ImportError:
    import json

import pytest

from plenum.recorder.src.recorder import Recorder

TestRunningTimeLimitSec = 350


def test_add_to_recorder(recorder):
    last_check_time = recorder.get_now_key()
    time.sleep(1)
    msg1, frm1 = 'm1', 'f1'
    msg2, frm2 = 'm2', 'f2'
    recorder.add_incoming(msg1, frm1)
    time.sleep(3)
    recorder.add_incoming(msg2, frm2)
    time.sleep(2.1)
    msg3, to1, to11 = 'm3', 't1', 't11'
    msg4, to2 = 'm4', 't2'
    recorder.add_outgoing(msg3, to1, to11)
    time.sleep(.4)
    recorder.add_outgoing(msg4, to2)
    i = 0
    for k, v in recorder.store.iterator(include_value=True):
        assert int(k.decode()) > int(last_check_time)

        if i == 0:
            assert v.decode() == json.dumps([[Recorder.INCOMING_FLAG, msg1, frm1]])

        if i == 1:
            assert v.decode() == json.dumps([[Recorder.INCOMING_FLAG, msg2, frm2]])
            assert int(k) - int(last_check_time) >= 3 * Recorder.TIME_FACTOR

        if i == 2:
            assert v.decode() == json.dumps([[Recorder.OUTGOING_FLAG, msg3, to1, to11]])
            assert int(k) - int(last_check_time) >= 2.1 * Recorder.TIME_FACTOR

        if i == 3:
            assert v.decode() == json.dumps([[Recorder.OUTGOING_FLAG, msg4, to2]])
            assert int(k) - int(last_check_time) >= .4 * Recorder.TIME_FACTOR

        last_check_time = k.decode()
        i += 1


def test_register_play_targets(recorder):
    l1 = []
    l2 = []

    def add1(arg):
        l1.append(arg)

    def add2(arg):
        l2.append(arg)

    assert not recorder.replay_targets
    recorder.register_replay_target('1', add1)
    assert len(recorder.replay_targets) == 1
    with pytest.raises(AssertionError):
        recorder.register_replay_target('1', add2)


def test_recorded_parsings(recorder):
    incoming = [[randomString(100), randomString(6)] for i in
                range(3)]
    outgoing = [[randomString(100), randomString(6)] for i in
                range(5)]
    for m, f in incoming:
        recorder.add_incoming(m, f)
        time.sleep(0.01)
    for m, f in outgoing:
        recorder.add_outgoing(m, f)
        time.sleep(0.01)

    with pytest.raises(AssertionError):
        recorder.get_parsed(incoming[0], only_incoming=True, only_outgoing=True)

    combined = incoming + outgoing

    for k, v in recorder.store.iterator(include_value=True):
        p = Recorder.get_parsed(v)
        assert [i[1:] for i in p] in combined
        p = Recorder.get_parsed(v, only_incoming=True)
        if p:
            assert p in incoming
            incoming.remove(p)
        p = Recorder.get_parsed(v, only_outgoing=True)
        if p:
            assert p in outgoing
            outgoing.remove(p)

    assert not incoming
    assert not outgoing


def test_recorder_get_next_incoming_only(recorder):
    incoming_count = 100
    incoming = [(randomString(100), randomString(6)) for _ in
                range(incoming_count)]

    while incoming:
        recorder.add_incoming(*incoming.pop())
        time.sleep(random.choice([0, 1]) + random.random())

    recorded_incomings = OrderedDict()
    keys = []
    for k, v in recorder.store.iterator(include_value=True):
        v = Recorder.get_parsed(v)
        keys.append(int(k))
        recorded_incomings[int(k)] = v

    assert len(recorded_incomings) == incoming_count
    assert sorted(keys) == keys

    max_time_to_run = incoming_count * 2 + 10
    recorder.start_playing()
    start = time.perf_counter()

    while recorder.is_playing and (time.perf_counter() < start + max_time_to_run):
        vals = recorder.get_next()
        if vals:
            check = recorded_incomings.popitem(last=False)[1]
            assert check == vals
        else:
            time.sleep(0.01)

    assert len(recorded_incomings) == 0
    assert not recorder.is_playing


def test_recorder_get_next(recorder):
    incoming_count = 100
    outgoing_count = 50

    incoming = [(randomString(100), randomString(6)) for _ in range(incoming_count)]
    outgoing = [(randomString(100), randomString(6)) for _ in range(outgoing_count)]

    while incoming or outgoing:
        if random.choice([0, 1]) and outgoing:
            recorder.add_outgoing(*outgoing.pop())
            time.sleep(random.choice([0, 1]) + random.random())
        elif incoming:
            recorder.add_incoming(*incoming.pop())
            time.sleep(random.choice([0, 1]) + random.random())
        else:
            continue

    recorded_incomings = OrderedDict()
    for k, v in recorder.store.iterator(include_value=True):
        v = Recorder.get_parsed(v, only_incoming=True)
        if v:
            recorded_incomings[int(k)] = v

    assert len(recorded_incomings) == incoming_count

    max_time_to_run = incoming_count * 2 + 10

    recorder.start_playing()
    start = time.perf_counter()

    while recorder.is_playing and (time.perf_counter() < start + max_time_to_run):
        vals = recorder.get_next()
        if vals:
            inc = Recorder.filter_incoming(vals)
            if inc:
                assert recorded_incomings.popitem(last=False)[1] == inc
        else:
            time.sleep(0.01)

    assert len(recorded_incomings) == 0
    assert not recorder.is_playing
