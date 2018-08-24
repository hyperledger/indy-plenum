import pytest

from plenum.common.moving_average import MovingAverage, ExponentialMovingAverage, EventFrequencyEstimator

START_TIME = 10
ESTIMATOR_WINDOW = 3


class MockMovingAverage(MovingAverage):
    def __init__(self):
        self.updates = []
        self.reset_called = False
        self._value = 0

    def update(self, value: float):
        self.updates.append(value)
        self._value = value

    def reset(self, value: float):
        self.reset_called = True
        self._value = 0

    @property
    def value(self) -> float:
        return self._value


@pytest.fixture(params=[ExponentialMovingAverage])
def ma_type(request):
    return request.param


@pytest.fixture()
def mock_averager():
    return MockMovingAverage()


@pytest.fixture()
def estimator(mock_averager):
    return EventFrequencyEstimator(start_time=START_TIME, window=ESTIMATOR_WINDOW, averager=mock_averager)


def create_moving_average(cls, start: float) -> MovingAverage:
    if cls == ExponentialMovingAverage:
        return cls(alpha=0.5, start=start)
    return None


def test_moving_average_has_initial_value_after_creation(ma_type):
    start = 4.2
    ma = create_moving_average(ma_type, start)
    assert ma.value == start


def test_moving_average_doesnt_change_value_when_fed_same_values(ma_type):
    start = 4.2
    ma = create_moving_average(ma_type, start)
    for _ in range(10):
        ma.update(start)
    assert ma.value == start


def test_moving_average_has_value_semantics_for_eq(ma_type):
    start = 4.2
    ma1 = create_moving_average(ma_type, start)
    ma2 = create_moving_average(ma_type, start)
    assert ma1 == ma2

    ma1.update(42)
    assert ma1 != ma2

    ma2.update(42)
    assert ma1 == ma2


def test_moving_average_resets_to_same_state_as_new(ma_type):
    ma = create_moving_average(ma_type, 1.3)
    for _ in range(10):
        ma.update(42)

    start = 4.2
    ma.reset(start)
    assert ma == create_moving_average(ma_type, start)


def test_moving_average_increases_value_when_updated_with_larger_value(ma_type):
    start = 4.2
    target = 42
    ma = create_moving_average(ma_type, start)

    for _ in range(10):
        last = ma.value
        ma.update(target)
        assert ma.value > last
        assert ma.value < target


def test_moving_average_decreases_value_when_updated_with_smaller_value(ma_type):
    start = 4.2
    target = -42
    ma = create_moving_average(ma_type, start)

    for _ in range(10):
        last = ma.value
        ma.update(target)
        assert ma.value < last
        assert ma.value > target


def test_moving_average_changes_faster_with_larger_difference_to_target(ma_type):
    ma1 = create_moving_average(ma_type, 10.0)
    ma2 = create_moving_average(ma_type, 20.0)

    for _ in range(10):
        last1 = ma1.value
        last2 = ma2.value
        ma1.update(18.0)
        ma2.update(22.0)
        assert ma1.value - last1 > ma2.value - last2


def test_exp_moving_average_converges_faster_with_larger_alpha():
    start = 4.2
    target = 42
    ma1 = ExponentialMovingAverage(0.2, start)
    ma2 = ExponentialMovingAverage(0.8, start)

    for _ in range(10):
        ma1.update(target)
        ma2.update(target)
        assert ma1.value < ma2.value


def test_exp_moving_average_moves_halfway_to_target_in_desired_number_of_steps():
    steps = 10
    alpha = ExponentialMovingAverage.halfway_alpha(steps)

    start = 4.2
    target = 42
    halfway = 0.5 * (start + target)
    ma = ExponentialMovingAverage(alpha, start)

    for i in range(steps - 1):
        ma.update(target)
        assert ma.value < halfway

    ma.update(target)
    assert ma.value > halfway


def test_event_frequency_estimator_is_initialized_to_zero(mock_averager, estimator):
    assert estimator.value == 0
    assert mock_averager.updates == []


def test_event_frequency_estimator_updates_with_time_even_if_there_are_no_events(mock_averager, estimator):
    estimator.update_time(START_TIME + 3.1 * ESTIMATOR_WINDOW)
    assert estimator.value == 0
    assert mock_averager.updates == [0, 0, 0]


def test_event_frequency_estimator_doesnt_updates_when_time_doesnt_advance_enough(mock_averager, estimator):
    estimator.add_events(3)
    estimator.update_time(START_TIME + 0.9 * ESTIMATOR_WINDOW)
    assert estimator.value == 0
    assert mock_averager.updates == []


def test_event_frequency_estimator_sums_all_events_in_same_window(mock_averager, estimator):
    estimator.add_events(3)
    estimator.update_time(START_TIME + 0.3 * ESTIMATOR_WINDOW)
    estimator.add_events(4)
    estimator.update_time(START_TIME + 1.2 * ESTIMATOR_WINDOW)
    estimator.add_events(2)
    assert estimator.value == 7
    assert mock_averager.updates == [7]


def test_event_frequency_estimator_doesnt_spread_events_between_windows(mock_averager, estimator):
    estimator.add_events(3)
    estimator.update_time(START_TIME + 0.3 * ESTIMATOR_WINDOW)
    estimator.add_events(4)
    estimator.update_time(START_TIME + 2.2 * ESTIMATOR_WINDOW)
    estimator.add_events(2)
    assert estimator.value == 0
    assert mock_averager.updates == [7, 0]


def test_event_frequency_estimator_resets_everything(mock_averager, estimator):
    estimator.add_events(3)
    estimator.update_time(START_TIME + 1.2 * ESTIMATOR_WINDOW)
    estimator.add_events(4)
    assert estimator.value == 3
    assert mock_averager.updates == [3]
    assert not mock_averager.reset_called

    estimator.reset(START_TIME + 3.0 * ESTIMATOR_WINDOW)
    assert estimator.value == 0
    assert mock_averager.reset_called

    estimator.add_events(2)
    estimator.update_time(START_TIME + 4.2 * ESTIMATOR_WINDOW)
    assert estimator.value == 2
    assert mock_averager.updates == [3, 2]
