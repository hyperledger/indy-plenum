import math
import statistics
import struct

from plenum.common.value_accumulator import ValueAccumulator


def test_value_accumulator_dont_return_anything_when_created():
    acc = ValueAccumulator()
    assert acc.count == 0
    assert acc.sum == 0
    assert acc.avg is None
    assert acc.stddev is None
    assert acc.min is None
    assert acc.max is None
    assert acc.lo is None
    assert acc.hi is None


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
    assert acc.lo == value
    assert acc.hi == value


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
    assert acc.lo == value
    assert acc.hi == value


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
    assert acc.min < acc.lo < acc.avg
    assert acc.avg < acc.hi < acc.max
    assert math.isclose(acc.hi - acc.lo, acc.stddev)


def test_value_accumulator_eq_has_value_semantics():
    a = ValueAccumulator()
    b = ValueAccumulator()
    assert a == b

    a.add(1.0)
    assert a != b

    b.add(1.0)
    assert a == b

    a.add(2.0)
    b.add(3.0)
    assert a != b


def test_value_accumulator_can_merge():
    values = [4.2, -1.3, 10.8]
    acc = ValueAccumulator(values)

    other_values = [3.7, 7.6, -8.5]
    other_acc = ValueAccumulator(other_values)

    all_values = values + other_values
    all_acc = ValueAccumulator(all_values)

    acc.merge(other_acc)
    assert acc == all_acc


def test_value_accumulator_can_be_stored_as_bytes():
    values = [4.2, -1.3, 10.8]
    acc = ValueAccumulator(values)

    data = acc.to_bytes()
    assert isinstance(data, bytes)

    restored_acc = ValueAccumulator.from_bytes(data)
    assert acc == restored_acc


def test_value_accumulator_with_one_value_is_stored_as_single_float64():
    acc = ValueAccumulator()
    acc.add(4.2)

    data = acc.to_bytes()
    assert struct.unpack('d', data)[0] == 4.2

    restored_acc = ValueAccumulator.from_bytes(data)
    assert acc == restored_acc


def test_value_accumulator_can_be_created_from_value():
    reference = ValueAccumulator()
    reference.add(4.2)

    acc = ValueAccumulator(4.2)

    assert acc == reference


def test_value_accumulator_can_be_created_from_list():
    reference = ValueAccumulator()
    reference.add(4.2)
    reference.add(1.3)

    acc = ValueAccumulator([4.2, 1.3])

    assert acc == reference
