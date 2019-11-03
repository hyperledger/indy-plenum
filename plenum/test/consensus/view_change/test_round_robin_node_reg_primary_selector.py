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


def test_view_change_primary_selection_constant_node_reg(primary_selector, validators, instance_count,
                                                         node_reg_handler):
    initial_view_no = 3
    node_reg_handler.uncommitted_node_reg = validators
    node_reg_handler.node_reg_at_beginning_of_view[initial_view_no - 1] = validators
    node_reg_handler.node_reg_at_beginning_of_view[initial_view_no - 2] = validators
    node_reg_handler.node_reg_at_beginning_of_view[initial_view_no] = validators

    primaries = set(primary_selector.select_primaries(initial_view_no, instance_count))
    prev_primaries = set(primary_selector.select_primaries(initial_view_no - 1, instance_count))
    next_primaries = set(primary_selector.select_primaries(initial_view_no + 1, instance_count))

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

# def test_select_primaries_for_view_1(primary_selector, node_reg_handler):
#     validators = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
#
#     primary_selector = RoundRobinConstantNodesPrimariesSelector(validators)
#     primaries = primary_selector.select_primaries(view_no=0,
#                                                   instance_count=3)
#     assert primaries == ["Alpha", "Beta", "Gamma"]