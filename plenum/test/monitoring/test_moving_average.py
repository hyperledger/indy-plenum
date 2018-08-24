import pytest

from plenum.common.moving_average import ExponentialMovingAverage


@pytest.fixture(params=[ExponentialMovingAverage])
def ma_type(request):
    return request.param


def create_moving_average(cls, start: float):
    if cls == ExponentialMovingAverage:
        return cls(start, alpha=0.5)
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
    ma1 = ExponentialMovingAverage(start, alpha=0.2)
    ma2 = ExponentialMovingAverage(start, alpha=0.8)

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
    ma = ExponentialMovingAverage(start, alpha)

    for i in range(steps - 1):
        ma.update(target)
        assert ma.value < halfway

    ma.update(target)
    assert ma.value > halfway
