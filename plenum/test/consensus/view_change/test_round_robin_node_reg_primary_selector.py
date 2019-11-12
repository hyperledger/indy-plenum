import pytest

from plenum.server.batch_handlers.node_reg_handler import NodeRegHandler
from plenum.server.consensus.primary_selector import RoundRobinNodeRegPrimariesSelector
from plenum.server.database_manager import DatabaseManager
from plenum.test.greek import genNodeNames


@pytest.fixture()
def node_reg_handler():
    return NodeRegHandler(DatabaseManager())


@pytest.fixture()
def primary_selector(node_reg_handler):
    return RoundRobinNodeRegPrimariesSelector(node_reg_handler)


@pytest.fixture()
def instance_count(validators):
    return (len(validators) - 1) // 3 + 1


@pytest.fixture(params=[4, 6, 7, 8])
def validators(request):
    return genNodeNames(request.param)


def test_view_change_primary_selection_dynamic_node_reg(primary_selector, validators, instance_count,
                                                         node_reg_handler):
    initial_view_no = 3
    node_reg_handler.uncommitted_node_reg = validators
    node_reg_handler.node_reg_at_beginning_of_view[initial_view_no - 1] = validators
    node_reg_handler.node_reg_at_beginning_of_view[initial_view_no - 2] = validators
    node_reg_handler.node_reg_at_beginning_of_view[initial_view_no] = validators

    instance_count = (len(validators) - 1) // 3 + 1
    primaries = set(primary_selector.select_primaries(initial_view_no))
    prev_primaries = set(primary_selector.select_primaries(initial_view_no - 1))
    next_primaries = set(primary_selector.select_primaries(initial_view_no + 1))

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


def test_select_primaries_for_view_0(primary_selector, node_reg_handler):
    node_reg_handler.node_reg_at_beginning_of_view[0] = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    primaries = primary_selector.select_primaries(view_no=0)
    assert primaries == ["Alpha", "Beta", "Gamma"]


@pytest.mark.parametrize('has_node_reg_last_view', [True, False])
def test_select_primaries_for_view_1_takes_node_reg_from_previous_view(primary_selector, node_reg_handler,
                                                                       has_node_reg_last_view):
    node_reg_handler.node_reg_at_beginning_of_view[0] = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    if has_node_reg_last_view:
        node_reg_handler.node_reg_at_beginning_of_view[1] = ["Epsilon", "Zeta", "Eta"]

    primaries = primary_selector.select_primaries(view_no=1)
    assert primaries == ["Beta", "Gamma", "Delta"]


@pytest.mark.parametrize('has_node_reg_last_view', [True, False])
def test_select_primaries_for_view_5_takes_node_reg_from_previous_view(primary_selector, node_reg_handler,
                                                                       has_node_reg_last_view):
    node_reg_handler.node_reg_at_beginning_of_view[4] = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    if has_node_reg_last_view:
        node_reg_handler.node_reg_at_beginning_of_view[5] = ["Epsilon", "Zeta", "Eta"]

    primaries = primary_selector.select_primaries(view_no=5)
    assert primaries == ["Zeta", "Eta", "Alpha"]


@pytest.mark.parametrize('has_node_reg_last_view', [True, False])
@pytest.mark.parametrize('prev_available_viewno', [0, 1, 2, 3])
def test_select_primaries_takes_latest_available_node_reg_for_previous_views(primary_selector, node_reg_handler,
                                                                             has_node_reg_last_view,
                                                                             prev_available_viewno):
    node_reg_handler.node_reg_at_beginning_of_view[prev_available_viewno] = ["Alpha", "Beta", "Gamma", "Delta",
                                                                             "Epsilon", "Zeta", "Eta"]
    if has_node_reg_last_view:
        node_reg_handler.node_reg_at_beginning_of_view[5] = ["Epsilon", "Zeta", "Eta"]

    primaries = primary_selector.select_primaries(view_no=5)
    assert primaries == ["Zeta", "Eta", "Alpha"]
