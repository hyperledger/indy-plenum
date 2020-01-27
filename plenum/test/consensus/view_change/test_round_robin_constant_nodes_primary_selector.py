import pytest

from plenum.server.consensus.primary_selector import RoundRobinConstantNodesPrimariesSelector
from plenum.test.greek import genNodeNames


@pytest.fixture(params=[4, 6, 7, 8])
def validators(request):
    return genNodeNames(request.param)


@pytest.fixture()
def primary_selector(validators):
    return RoundRobinConstantNodesPrimariesSelector(validators)


def test_view_change_primary_selection(primary_selector, initial_view_no, validators):
    primaries = set(primary_selector.select_primaries(initial_view_no))
    prev_primaries = set(primary_selector.select_primaries(initial_view_no - 1))
    next_primaries = set(primary_selector.select_primaries(initial_view_no + 1))

    instance_count = (len(validators) - 1) // 3 + 1
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
    primary_selector = RoundRobinConstantNodesPrimariesSelector(validators)
    master_primary = primary_selector.select_master_primary(view_no=0)
    primaries = primary_selector.select_primaries(view_no=0)
    assert master_primary == "Alpha"
    assert primaries == ["Alpha", "Beta", "Gamma"]


def test_primaries_selection_viewno_5(primary_selector):
    validators = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    primary_selector = RoundRobinConstantNodesPrimariesSelector(validators)
    master_primary = primary_selector.select_master_primary(view_no=5)
    primaries = primary_selector.select_primaries(view_no=5)
    assert master_primary == "Zeta"
    assert primaries == ["Zeta", "Eta", "Alpha"]


def test_primaries_selection_viewno_9(primary_selector):
    validators = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    primary_selector = RoundRobinConstantNodesPrimariesSelector(validators)
    master_primary = primary_selector.select_master_primary(view_no=9)
    primaries = primary_selector.select_primaries(view_no=9)
    assert master_primary == "Gamma"
    assert primaries == ["Gamma", "Delta", "Epsilon"]
