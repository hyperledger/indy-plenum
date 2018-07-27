from plenum.common.metrics_stats import ValueAccumulator
import statistics


def test_value_accumulator_dont_return_anything_when_created():
    acc = ValueAccumulator()
    assert acc.count == 0
    assert acc.sum == 0
    assert acc.avg is None
    assert acc.stddev is None
    assert acc.min is None
    assert acc.max is None


def test_value_accumulator_can_add_value():
    value = 4.2
    acc = ValueAccumulator()
    acc.add(value)
    assert acc.count == 1
    assert acc.sum == value
    assert acc.avg == value
    assert acc.stddev is None
    assert acc.min == value
    assert acc.max == value


def test_value_accumulator_handles_same_values():
    value = 4.2
    count = 5
    acc = ValueAccumulator()
    for _ in range(count):
        acc.add(value)

    assert acc.count == count
    assert acc.sum == value * count
    assert acc.avg == value
    assert acc.stddev == 0
    assert acc.min == value
    assert acc.max == value


def test_value_accumulator_can_add_several_values():
    values = [4.2, -1.3, 10.8]
    acc = ValueAccumulator()
    for value in values:
        acc.add(value)

    assert acc.count == len(values)
    assert acc.sum == sum(values)
    assert acc.avg == statistics.mean(values)
    assert acc.stddev == statistics.stdev(values)
    assert acc.min == min(values)
    assert acc.max == max(values)


def test_value_accumulator_can_merge():
    values = [4.2, -1.3, 10.8]
    acc = ValueAccumulator()
    for value in values:
        acc.add(value)

    other_values = [3.7, 7.6, -8.5]
    other_acc = ValueAccumulator()
    for value in other_values:
        other_acc.add(value)

    acc.merge(other_acc)
    all_values = values + other_values
    assert acc.count == len(all_values)
    assert acc.sum == sum(all_values)
    assert acc.avg == statistics.mean(all_values)
    assert acc.stddev == statistics.stdev(all_values)
    assert acc.min == min(all_values)
    assert acc.max == max(all_values)

