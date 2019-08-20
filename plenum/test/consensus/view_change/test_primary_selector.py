from plenum.server.consensus.primary_selector import RoundRobinPrimariesSelector

import pytest

from plenum.test.greek import genNodeNames


@pytest.fixture()
def primary_selector():
    return RoundRobinPrimariesSelector()


@pytest.fixture()
def instance_count(validators):
    return (len(validators) - 1) // 3 + 1


@pytest.fixture(params=[4, 6, 7, 8])
def validators(request):
    return genNodeNames(request.param)


def test_view_change_primary_selection(primary_selector, validators, instance_count, initial_view_no):
    primaries = set(primary_selector.select_primaries(initial_view_no, instance_count, validators))
    prev_primaries = set(primary_selector.select_primaries(initial_view_no - 1, instance_count, validators))
    next_primaries = set(primary_selector.select_primaries(initial_view_no + 1, instance_count, validators))

    assert len(set(primaries)) == instance_count
    assert len(set(prev_primaries)) == instance_count
    assert len(set(next_primaries)) == instance_count

    assert primaries.issubset(validators)
    assert prev_primaries.issubset(validators)
    assert next_primaries.issubset(validators)

    assert primaries != prev_primaries
    assert primaries != next_primaries

    assert len(primaries & prev_primaries) == instance_count - 1
    assert len(primaries & next_primaries) == instance_count - 1
    assert len(prev_primaries & next_primaries) == instance_count - 2
    assert len(primaries | prev_primaries) == instance_count + 1
    assert len(primaries | next_primaries) == instance_count + 1
    assert len(prev_primaries | next_primaries) == instance_count + 2


def test_primaries_selection_viewno_0(primary_selector):
    validators = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    primaries = primary_selector.select_primaries(view_no=0,
                                                  instance_count=3,
                                                  validators=validators)
    assert primaries == ["Alpha", "Beta", "Gamma"]


def test_primaries_selection_viewno_5(primary_selector):
    validators = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    primaries = primary_selector.select_primaries(view_no=5,
                                                  instance_count=3,
                                                  validators=validators)
    assert primaries == ["Zeta", "Eta", "Alpha"]


def test_primaries_selection_viewno_9(primary_selector):
    validators = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    primaries = primary_selector.select_primaries(view_no=9,
                                                  instance_count=3,
                                                  validators=validators)
    assert primaries == ["Gamma", "Delta", "Epsilon"]
