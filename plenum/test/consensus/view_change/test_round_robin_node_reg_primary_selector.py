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


@pytest.mark.parametrize('has_node_reg_last_view', [True, False])
def test_select_primaries_for_view_0(primary_selector, node_reg_handler, has_node_reg_last_view):
    node_reg = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    node_reg_handler.uncommitted_node_reg = node_reg
    node_reg_handler.node_reg_at_beginning_of_view[0] = node_reg

    # committed and beginning of current view shouldn't be taking into account so can be any
    node_reg_handler.committed_node_reg = ['AAA', 'BBB', 'CCC']
    if has_node_reg_last_view:
        node_reg_handler.node_reg_at_beginning_of_view[1] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.node_reg_at_beginning_of_view[2] = ['AAA', 'BBB', 'CCC']

    primaries = primary_selector.select_primaries(view_no=0)
    assert primaries == ["Alpha", "Beta", "Gamma"]


@pytest.mark.parametrize('has_node_reg_last_view', [True, False])
@pytest.mark.parametrize('uncommitted_node_reg',
                         [list(range(i)) for i in range(3, 30)])
def test_select_primaries_for_view_1_takes_node_reg_from_previous_view_same_node_reg_lenghth(primary_selector,
                                                                                             node_reg_handler,
                                                                                             has_node_reg_last_view,
                                                                                             uncommitted_node_reg):
    node_reg_handler.uncommitted_node_reg = uncommitted_node_reg
    node_reg_handler.node_reg_at_beginning_of_view[0] = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]

    # committed and beginning of current view shouldn't be taking into account so can be any
    node_reg_handler.committed_node_reg = ['AAA', 'BBB', 'CCC']
    if has_node_reg_last_view:
        node_reg_handler.node_reg_at_beginning_of_view[1] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.node_reg_at_beginning_of_view[2] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.node_reg_at_beginning_of_view[3] = ['AAA', 'BBB', 'CCC']

    primaries = primary_selector.select_primaries(view_no=1)

    expected_instance_count = (len(uncommitted_node_reg) - 1) // 3 + 1
    if expected_instance_count == 10:
        assert primaries == ["Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta"]
    elif expected_instance_count == 9:
        assert primaries == ["Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Alpha", "Beta", "Gamma"]
    elif expected_instance_count == 8:
        assert primaries == ["Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Alpha", "Beta"]
    elif expected_instance_count == 7:
        assert primaries == ["Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Alpha"]
    elif expected_instance_count == 6:
        assert primaries == ["Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    elif expected_instance_count == 5:
        assert primaries == ["Beta", "Gamma", "Delta", "Epsilon", "Zeta"]
    elif expected_instance_count == 4:
        assert primaries == ["Beta", "Gamma", "Delta", "Epsilon"]
    elif expected_instance_count == 3:
        assert primaries == ["Beta", "Gamma", "Delta"]
    elif expected_instance_count == 2:
        assert primaries == ["Beta", "Gamma"]
    elif expected_instance_count == 1:
        assert primaries == ["Beta"]


@pytest.mark.parametrize('has_node_reg_last_view', [True, False])
@pytest.mark.parametrize('uncommitted_node_reg',
                         [list(range(i)) for i in range(3, 30)])
def test_select_primaries_for_view_5_takes_node_reg_from_previous_view(primary_selector, node_reg_handler,
                                                                       has_node_reg_last_view,
                                                                       uncommitted_node_reg):
    node_reg_handler.uncommitted_node_reg = uncommitted_node_reg
    node_reg_handler.node_reg_at_beginning_of_view[4] = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]

    # committed and beginning of current view shouldn't be taking into account so can be any
    node_reg_handler.committed_node_reg = ['AAA', 'BBB', 'CCC']
    if has_node_reg_last_view:
        node_reg_handler.node_reg_at_beginning_of_view[5] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.node_reg_at_beginning_of_view[6] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.node_reg_at_beginning_of_view[7] = ['AAA', 'BBB', 'CCC']

    primaries = primary_selector.select_primaries(view_no=5)

    expected_instance_count = (len(uncommitted_node_reg) - 1) // 3 + 1
    if expected_instance_count == 10:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Alpha"]
    elif expected_instance_count == 9:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    elif expected_instance_count == 8:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta"]
    elif expected_instance_count == 7:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta", "Epsilon"]
    elif expected_instance_count == 6:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta"]
    elif expected_instance_count == 5:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma"]
    elif expected_instance_count == 4:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta"]
    elif expected_instance_count == 3:
        assert primaries == ["Zeta", "Eta", "Alpha"]
    elif expected_instance_count == 2:
        assert primaries == ["Zeta", "Eta"]
    elif expected_instance_count == 1:
        assert primaries == ["Zeta"]


@pytest.mark.parametrize('has_node_reg_last_view', [True, False])
@pytest.mark.parametrize('prev_available_viewno', [0, 1, 2, 3])
@pytest.mark.parametrize('uncommitted_node_reg',
                         [list(range(i)) for i in range(3, 30)])
def test_select_primaries_takes_latest_available_node_reg_for_previous_views(primary_selector, node_reg_handler,
                                                                             has_node_reg_last_view,
                                                                             prev_available_viewno,
                                                                             uncommitted_node_reg):
    node_reg_handler.uncommitted_node_reg = uncommitted_node_reg
    node_reg_handler.node_reg_at_beginning_of_view[prev_available_viewno] = ["Alpha", "Beta", "Gamma", "Delta",
                                                                             "Epsilon", "Zeta", "Eta"]

    # committed and beginning of current view shouldn't be taking into account so can be any
    node_reg_handler.committed_node_reg = ['AAA', 'BBB', 'CCC']
    if has_node_reg_last_view:
        node_reg_handler.node_reg_at_beginning_of_view[5] = ['AAA', 'BBB', 'CCC']

    primaries = primary_selector.select_primaries(view_no=5)

    expected_instance_count = (len(uncommitted_node_reg) - 1) // 3 + 1
    if expected_instance_count == 10:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Alpha"]
    elif expected_instance_count == 9:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta"]
    elif expected_instance_count == 8:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta"]
    elif expected_instance_count == 7:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta", "Epsilon"]
    elif expected_instance_count == 6:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma", "Delta"]
    elif expected_instance_count == 5:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta", "Gamma"]
    elif expected_instance_count == 4:
        assert primaries == ["Zeta", "Eta", "Alpha", "Beta"]
    elif expected_instance_count == 3:
        assert primaries == ["Zeta", "Eta", "Alpha"]
    elif expected_instance_count == 2:
        assert primaries == ["Zeta", "Eta"]
    elif expected_instance_count == 1:
        assert primaries == ["Zeta"]
