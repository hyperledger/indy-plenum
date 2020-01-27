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
    node_reg_handler.committed_node_reg_at_beginning_of_view[initial_view_no - 1] = validators
    node_reg_handler.committed_node_reg_at_beginning_of_view[initial_view_no - 2] = validators
    node_reg_handler.committed_node_reg_at_beginning_of_view[initial_view_no] = validators
    node_reg_handler.uncommitted_node_reg_at_beginning_of_view = node_reg_handler.committed_node_reg_at_beginning_of_view

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


@pytest.mark.parametrize('has_node_reg_next_view', [True, False])
def test_select_primaries_for_view_0(primary_selector, node_reg_handler, has_node_reg_next_view):
    node_reg_handler.committed_node_reg_at_beginning_of_view[0] = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta",
                                                                   "Eta"]
    node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] = ["Alpha", "Delta", "Epsilon", "Zeta", "Eta",
                                                                     "Kappa"]

    # committed and uncommitted_node_reg can be any
    node_reg_handler.committed_node_reg = ['AAA', 'BBB', 'CCC']
    node_reg_handler.uncommitted_node_reg = ['AAA', 'BBB', 'CCC']
    if has_node_reg_next_view:
        node_reg_handler.committed_node_reg_at_beginning_of_view[1] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.committed_node_reg_at_beginning_of_view[2] = ['AAA', 'BBB', 'CCC']

    master_primary = primary_selector.select_master_primary(view_no=0)
    primaries = primary_selector.select_primaries(view_no=0)
    assert master_primary == "Alpha"
    assert primaries == ["Alpha", "Delta"]


@pytest.mark.parametrize('has_node_reg_next_view', [True, False])
def test_select_primaries_for_view_1_takes_node_reg_from_previous_view(primary_selector,
                                                                       node_reg_handler,
                                                                       has_node_reg_next_view
                                                                       ):
    node_reg_handler.committed_node_reg_at_beginning_of_view[0] = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta",
                                                                   "Eta"]
    node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] = ["Alpha", "Delta", "Epsilon", "Zeta", "Eta",
                                                                     "Kappa", "Gamma"]

    # committed and uncommitted_node_reg can be any
    node_reg_handler.committed_node_reg = ['AAA', 'BBB', 'CCC']
    node_reg_handler.uncommitted_node_reg = ['AAA', 'BBB', 'CCC']
    if has_node_reg_next_view:
        node_reg_handler.committed_node_reg_at_beginning_of_view[1] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.committed_node_reg_at_beginning_of_view[2] = ['AAA', 'BBB', 'CCC']

    master_primary = primary_selector.select_master_primary(view_no=1)
    primaries = primary_selector.select_primaries(view_no=1)
    assert master_primary == "Beta"
    assert primaries == ["Delta", "Epsilon", "Zeta"]


@pytest.mark.parametrize('has_node_reg_next_view', [True, False])
def test_select_primaries_for_view_greater_than_node_reg(primary_selector,
                                                         node_reg_handler,
                                                         has_node_reg_next_view
                                                         ):
    node_reg_handler.committed_node_reg_at_beginning_of_view[8] = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta",
                                                                   "Eta"]
    node_reg_handler.uncommitted_node_reg_at_beginning_of_view[8] = ["Alpha", "Delta", "Epsilon", "Zeta", "Eta",
                                                                     "Kappa", "Beta"]

    # committed and uncommitted_node_reg can be any
    node_reg_handler.committed_node_reg = ['AAA', 'BBB', 'CCC']
    node_reg_handler.uncommitted_node_reg = ['AAA', 'BBB', 'CCC']
    if has_node_reg_next_view:
        node_reg_handler.committed_node_reg_at_beginning_of_view[7] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.committed_node_reg_at_beginning_of_view[9] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.committed_node_reg_at_beginning_of_view[10] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.uncommitted_node_reg_at_beginning_of_view[6] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.uncommitted_node_reg_at_beginning_of_view[7] = ['AAA', 'BBB', 'CCC']

    master_primary = primary_selector.select_master_primary(view_no=9)
    primaries = primary_selector.select_primaries(view_no=9)
    assert master_primary == "Gamma"
    assert primaries == ["Epsilon", "Zeta", "Eta"]


@pytest.mark.parametrize('has_node_reg_next_view', [True, False])
def test_select_primaries_dont_select_equal_master_and_backup(primary_selector,
                                                              node_reg_handler,
                                                              has_node_reg_next_view
                                                              ):
    node_reg_handler.committed_node_reg_at_beginning_of_view[5] = ["Alpha", "Beta", "Gamma", "Delta"]
    node_reg_handler.uncommitted_node_reg_at_beginning_of_view[5] = ["Beta", "Alpha", "Gamma", "Delta", "Epsilon"]

    # committed and uncommitted_node_reg can be any
    node_reg_handler.committed_node_reg = ['AAA', 'BBB', 'CCC']
    node_reg_handler.uncommitted_node_reg = ['AAA', 'BBB', 'CCC']
    if has_node_reg_next_view:
        node_reg_handler.committed_node_reg_at_beginning_of_view[7] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.committed_node_reg_at_beginning_of_view[9] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.committed_node_reg_at_beginning_of_view[10] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.uncommitted_node_reg_at_beginning_of_view[4] = ['AAA', 'BBB', 'CCC']

    master_primary = primary_selector.select_master_primary(view_no=6)
    primaries = primary_selector.select_primaries(view_no=6)
    assert master_primary == "Gamma"
    assert primaries == ["Alpha", "Gamma"]


@pytest.mark.parametrize('has_node_reg_next_view', [True, False])
def test_select_primaries_num_of_replicas_from_active_node_reg(primary_selector,
                                                               node_reg_handler,
                                                               has_node_reg_next_view
                                                               ):
    node_reg_handler.committed_node_reg_at_beginning_of_view[3] = ["Alpha", "Beta", "Gamma"]
    node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon",
                                                                     "Zeta", "Eta"]

    # committed and uncommitted_node_reg can be any
    node_reg_handler.committed_node_reg = ['AAA', 'BBB', 'CCC']
    node_reg_handler.uncommitted_node_reg = ['AAA', 'BBB', 'CCC']
    if has_node_reg_next_view:
        node_reg_handler.committed_node_reg_at_beginning_of_view[7] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.committed_node_reg_at_beginning_of_view[9] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.committed_node_reg_at_beginning_of_view[10] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] = ['AAA', 'BBB', 'CCC']

    master_primary = primary_selector.select_master_primary(view_no=4)
    primaries = primary_selector.select_primaries(view_no=4)
    assert master_primary == "Beta"
    assert primaries == ["Epsilon", "Zeta", "Eta"]


@pytest.mark.parametrize('has_node_reg_next_view', [True, False])
@pytest.mark.parametrize('prev_available_viewno', [0, 1, 2, 3])
def test_select_primaries_takes_latest_available_node_reg_for_previous_views(primary_selector, node_reg_handler,
                                                                             has_node_reg_next_view,
                                                                             prev_available_viewno):
    node_reg_handler.committed_node_reg_at_beginning_of_view[prev_available_viewno] = ["Alpha", "Beta", "Gamma",
                                                                                       "Delta",
                                                                                       "Epsilon", "Zeta", "Eta"]
    node_reg_handler.uncommitted_node_reg_at_beginning_of_view[prev_available_viewno] = ["Alpha", "Beta", "Gamma",
                                                                                         "Delta"]

    # committed and uncommitted_node_reg can be any
    node_reg_handler.committed_node_reg = ['AAA', 'BBB', 'CCC']
    node_reg_handler.uncommitted_node_reg = ['AAA', 'BBB', 'CCC']
    if has_node_reg_next_view:
        node_reg_handler.committed_node_reg_at_beginning_of_view[7] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.committed_node_reg_at_beginning_of_view[9] = ['AAA', 'BBB', 'CCC']
        node_reg_handler.committed_node_reg_at_beginning_of_view[10] = ['AAA', 'BBB', 'CCC']

    master_primary = primary_selector.select_master_primary(view_no=4)
    primaries = primary_selector.select_primaries(view_no=4)
    assert master_primary == "Epsilon"
    assert primaries == ["Alpha", "Beta"]
