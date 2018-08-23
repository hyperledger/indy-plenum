from math import isclose

import pytest

from plenum.server.monitor import RevivalSpikeResistantEMAThroughputMeasurement


State = RevivalSpikeResistantEMAThroughputMeasurement.State


def test_rsr_ema_throughput_measurement_creation():
    tm = RevivalSpikeResistantEMAThroughputMeasurement(window_size=5,
                                                       min_cnt=9)

    assert tm.window_size == 5
    assert tm.min_cnt == 9
    assert isclose(tm.alpha, 0.2)

    assert tm.state == State.FADED

    assert tm.window_start_ts is None  # not initiated in `__init__`
    assert tm.reqs_in_window == 0
    assert tm.throughput == 0

    assert tm.throughput_before_idle == 0
    assert tm.idle_start_ts is None  # not initiated in `__init__`
    assert tm.empty_windows_count == 0


def test_rsr_ema_throughput_measurement_init_time():
    tm = RevivalSpikeResistantEMAThroughputMeasurement(window_size=5,
                                                       min_cnt=9)
    tm.init_time(321.5)

    assert tm.state == State.FADED
    assert tm.window_start_ts == 321.5
    assert tm.idle_start_ts == 321.5


@pytest.fixture(scope="function")
def tm():
    throughput_measurement = \
        RevivalSpikeResistantEMAThroughputMeasurement(window_size=15,
                                                      min_cnt=16)
    throughput_measurement.init_time(0)
    return throughput_measurement


def test_rsr_ema_tm_past_windows_processing_on_add_request(tm):
    # [0, 15)
    tm.add_request(1)
    tm.add_request(5)
    tm.add_request(8)
    assert tm.window_start_ts == 0
    assert tm.reqs_in_window == 3

    # [15, 30)
    tm.add_request(15)
    assert tm.window_start_ts == 15
    assert tm.reqs_in_window == 1

    tm.add_request(16)
    assert tm.window_start_ts == 15
    assert tm.reqs_in_window == 2

    # [30, 45)
    tm.add_request(42)
    assert tm.window_start_ts == 30
    assert tm.reqs_in_window == 1


def test_rsr_ema_tm_past_windows_processing_on_get_throughput(tm):
    # [0, 15)
    tm.add_request(1)
    tm.add_request(5)
    tm.add_request(8)
    assert tm.window_start_ts == 0
    assert tm.reqs_in_window == 3

    tm.get_throughput(14)
    assert tm.window_start_ts == 0
    assert tm.reqs_in_window == 3

    # [15, 30)
    tm.get_throughput(15)
    assert tm.window_start_ts == 15
    assert tm.reqs_in_window == 0

    tm.add_request(16)
    tm.get_throughput(16)
    assert tm.window_start_ts == 15
    assert tm.reqs_in_window == 1

    # [30, 45)
    tm.get_throughput(42)
    assert tm.window_start_ts == 30
    assert tm.reqs_in_window == 0


@pytest.fixture(scope="function")
def tm_after_start(tm):
    assert tm.state == State.FADED
    return tm


def test_rsr_ema_tm_after_start_stays_in_faded_while_windows_are_empty(tm_after_start):
    tm = tm_after_start

    # [0, 15) - [45, 60)

    # [60, 75)
    throughput = tm.get_throughput(62)

    assert tm.state == State.FADED
    assert throughput == 0

    assert tm.throughput_before_idle == 0
    assert tm.idle_start_ts == 0
    assert tm.empty_windows_count == 4


def test_rsr_ema_tm_after_start_switches_to_revival_on_not_empty_window(tm_after_start):
    tm = tm_after_start

    # [0, 15) - [30, 45)

    # [45, 60)
    tm.add_request(50)

    # [60, 75)
    throughput = tm.get_throughput(62)

    assert tm.state == State.REVIVAL
    assert throughput is None

    assert tm.throughput_before_idle == 0
    assert tm.idle_start_ts == 0
    assert tm.empty_windows_count == 3

    assert tm.revival_start_ts == 45
    assert tm.revival_windows_count == 1
    assert tm.reqs_during_revival == 1


def test_rsr_ema_tm_after_start_switches_to_revival_if_first_window_is_not_empty(tm_after_start):
    tm = tm_after_start

    # [0, 15)
    tm.add_request(0)

    # [15, 30)
    throughput = tm.get_throughput(15)

    assert tm.state == State.REVIVAL
    assert throughput is None

    assert tm.throughput_before_idle == 0
    assert tm.idle_start_ts == 0
    assert tm.empty_windows_count == 0

    assert tm.revival_start_ts == 0
    assert tm.revival_windows_count == 1
    assert tm.reqs_during_revival == 1


@pytest.fixture(scope="function")
def tm_in_normal(tm_after_start):
    tm = tm_after_start

    # [0, 15)
    for ts in range(0, 15, 5):
        tm.add_request(ts)

    # [15, 30) - [225, 240) -- up to 16 windows
    tm.get_throughput(15)
    assert tm.state == State.REVIVAL

    for ts in range(15, 240, 5):
        tm.add_request(ts)

    # [240, 255)
    tm.get_throughput(240)
    assert tm.state == State.NORMAL

    return tm


def test_rsr_ema_tm_in_normal_stays_in_normal_while_windows_are_not_empty(tm_in_normal):
    tm = tm_in_normal

    # [240, 255)
    for ts in range(240, 255, 5):
        tm.add_request(ts)

    # [255, 270)
    tm.add_request(255)

    # [270, 285)
    throughput = tm.get_throughput(272)

    assert tm.state == State.NORMAL
    assert throughput is not None


def test_rsr_ema_tm_in_normal_switches_to_idle_on_empty_window(tm_in_normal):
    tm = tm_in_normal

    # [240, 255)
    tm.add_request(240)
    tm.add_request(245)

    # [255, 270)
    throughput_gotten_before_idle = tm.get_throughput(269)

    # [270, 285)
    throughput = tm.get_throughput(272)

    assert tm.state == State.IDLE
    assert throughput is not None

    assert tm.throughput_before_idle is not None
    assert tm.throughput_before_idle == throughput_gotten_before_idle
    assert tm.idle_start_ts == 255
    assert tm.empty_windows_count == 1


@pytest.fixture(scope="function")
def tm_in_idle_and_throughput_gotten_before_idle(tm_in_normal):
    tm = tm_in_normal

    # [240, 255) - [285, 300)
    for ts in range(240, 300, 5):
        tm.add_request(ts)

    # [300, 315)
    throughput_gotten_before_idle = tm.get_throughput(300)
    assert tm.state == State.NORMAL

    # [315, 330)
    tm.get_throughput(315)
    assert tm.state == State.IDLE

    assert tm.idle_start_ts == 300

    return tm, throughput_gotten_before_idle


def test_rsr_ema_tm_in_idle_stays_in_idle_while_windows_empty_and_less_min_cnt(
        tm_in_idle_and_throughput_gotten_before_idle):

    tm, throughput_gotten_before_idle = tm_in_idle_and_throughput_gotten_before_idle

    # [315, 330) - [510, 525) -- up to 15 empty windows

    # [525, 540)
    throughput = tm.get_throughput(531)

    assert tm.state == State.IDLE
    assert throughput is not None

    assert tm.throughput_before_idle == throughput_gotten_before_idle
    assert tm.idle_start_ts == 300
    assert tm.empty_windows_count == 15


def test_rsr_ema_tm_in_idle_switches_to_normal_on_not_empty_window(
        tm_in_idle_and_throughput_gotten_before_idle):

    tm, _ = tm_in_idle_and_throughput_gotten_before_idle

    # [315, 330) - [345, 360)

    # [360, 375)
    tm.add_request(370)

    # [375, 390)
    throughput = tm.get_throughput(381)

    assert tm.state == State.NORMAL
    assert throughput is not None


def test_rsr_ema_tm_in_idle_switches_to_faded_on_min_cnt_empty_windows(
        tm_in_idle_and_throughput_gotten_before_idle):

    tm, throughput_gotten_before_idle = tm_in_idle_and_throughput_gotten_before_idle

    # [315, 330) - [525, 540) -- up to 16 empty windows

    # [540, 555)
    throughput = tm.get_throughput(540)

    assert tm.state == State.FADED
    assert throughput is not None

    assert tm.throughput_before_idle == throughput_gotten_before_idle
    assert tm.idle_start_ts == 300
    assert tm.empty_windows_count == 16


@pytest.fixture(scope="function")
def tm_in_faded_and_throughput_gotten_before_idle(tm_in_idle_and_throughput_gotten_before_idle):
    tm, throughput_gotten_before_idle = tm_in_idle_and_throughput_gotten_before_idle

    # [315, 330) - [525, 540) -- up to 16 empty windows

    # [540, 555)
    throughput = tm.get_throughput(540)
    assert tm.state == State.FADED

    assert tm.idle_start_ts == 300

    return tm, throughput_gotten_before_idle


def test_rsr_ema_tm_in_faded_stays_in_faded_while_windows_are_empty(
        tm_in_faded_and_throughput_gotten_before_idle):

    tm, throughput_gotten_before_idle = tm_in_faded_and_throughput_gotten_before_idle

    # [540, 555) - [585, 600)

    # [600, 615)
    throughput = tm.get_throughput(600)

    assert tm.state == State.FADED
    assert throughput is not None

    assert tm.throughput_before_idle == throughput_gotten_before_idle
    assert tm.idle_start_ts == 300
    assert tm.empty_windows_count == 20


def test_rsr_ema_tm_in_faded_switches_to_revival_on_not_empty_window(
        tm_in_faded_and_throughput_gotten_before_idle):

    tm, throughput_gotten_before_idle = tm_in_faded_and_throughput_gotten_before_idle

    # [540, 555) - [570, 585)

    # [585, 600)
    tm.add_request(590)
    tm.add_request(595)

    # [600, 615)
    throughput = tm.get_throughput(600)

    assert tm.state == State.REVIVAL
    assert throughput is None

    assert tm.throughput_before_idle == throughput_gotten_before_idle
    assert tm.idle_start_ts == 300
    assert tm.empty_windows_count == 19

    assert tm.revival_start_ts == 585
    assert tm.revival_windows_count == 1
    assert tm.reqs_during_revival == 2


@pytest.fixture(scope="function")
def tm_in_revival_and_throughput_gotten_before_idle(tm_in_faded_and_throughput_gotten_before_idle):
    tm, throughput_gotten_before_idle = tm_in_faded_and_throughput_gotten_before_idle

    # [540, 555) - [585, 600)

    # [600, 615)
    for ts in range(600, 615, 5):
        tm.add_request(ts)

    # [615, 630)
    tm.get_throughput(615)
    assert tm.state == State.REVIVAL

    assert tm.idle_start_ts == 300
    assert tm.revival_start_ts == 600

    return tm, throughput_gotten_before_idle


def test_rsr_ema_tm_in_revival_stays_in_revival_while_windows_not_empty_and_less_min_cnt(
        tm_in_revival_and_throughput_gotten_before_idle):

    tm, throughput_gotten_before_idle = tm_in_revival_and_throughput_gotten_before_idle

    # [615, 630) - [810, 825) -- up to 15 not empty windows
    for ts in range(615, 825, 5):
        tm.add_request(ts)

    # [825, 840)
    throughput = tm.get_throughput(829)

    assert tm.state == State.REVIVAL
    assert throughput is None

    assert tm.throughput_before_idle == throughput_gotten_before_idle
    assert tm.idle_start_ts == 300
    assert tm.empty_windows_count == 20

    assert tm.revival_start_ts == 600
    assert tm.revival_windows_count == 15
    assert tm.reqs_during_revival == 45


def test_rsr_ema_tm_in_revival_switches_to_normal_on_min_cnt_not_empty_windows(
        tm_in_revival_and_throughput_gotten_before_idle):

    tm, _ = tm_in_revival_and_throughput_gotten_before_idle

    # [615, 630) - [810, 840) -- up to 16 not empty windows
    for ts in range(615, 840, 5):
        tm.add_request(ts)

    # [840, 855)
    throughput = tm.get_throughput(840)

    assert tm.state == State.NORMAL
    assert throughput is not None


def test_rsr_ema_tm_in_revival_switches_to_idle_on_empty_window(
        tm_in_revival_and_throughput_gotten_before_idle):

    tm, _ = tm_in_revival_and_throughput_gotten_before_idle

    # [615, 630)
    tm.add_request(615)

    # [630, 645)

    # [645, 660)
    throughput = tm.get_throughput(649)

    assert tm.state == State.IDLE
    assert throughput is not None

    assert tm.throughput_before_idle is not None
    assert tm.throughput_before_idle > throughput
    assert tm.idle_start_ts == 630
    assert tm.empty_windows_count == 1
