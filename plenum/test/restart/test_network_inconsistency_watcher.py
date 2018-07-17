import pytest

from plenum.server.inconsistency_watchers import NetworkInconsistencyWatcher

DEFAULT_NODE_SET = {'Alpha', 'Beta', 'Gamma', 'Delta'}


class WatcherCallbackMock:
    def __init__(self):
        self.call_count = 0

    def __call__(self):
        self.call_count += 1


@pytest.fixture
def watcher():
    cb = WatcherCallbackMock()
    watcher = NetworkInconsistencyWatcher(cb)
    watcher.set_nodes(DEFAULT_NODE_SET)
    return watcher


def _add_node(watcher: NetworkInconsistencyWatcher, node: str):
    nodes = watcher.nodes
    nodes.add(node)
    watcher.set_nodes(nodes)


def _remove_node(watcher: NetworkInconsistencyWatcher, node: str):
    nodes = watcher.nodes
    nodes.discard(node)
    watcher.set_nodes(nodes)


def test_watcher_is_not_triggered_when_created(watcher: NetworkInconsistencyWatcher):
    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_nodes_are_initially_connected(watcher: NetworkInconsistencyWatcher):
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.connect('Gamma')
    watcher.connect('Delta')

    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_just_one_node_connects_and_disconnects(watcher: NetworkInconsistencyWatcher):
    watcher.connect('Alpha')
    watcher.disconnect('Alpha')

    assert watcher.callback.call_count == 0


def test_watcher_is_triggered_when_going_below_consensus(watcher: NetworkInconsistencyWatcher):
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.connect('Gamma')
    watcher.disconnect('Beta')
    watcher.disconnect('Gamma')

    assert watcher.callback.call_count == 1


def test_watcher_is_not_triggered_when_going_below_consensus_without_going_above_strong_quorum(watcher: NetworkInconsistencyWatcher):
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.disconnect('Beta')

    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_adding_nodes_while_on_edge_of_consensus(watcher: NetworkInconsistencyWatcher):
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.connect('Gamma')
    watcher.disconnect('Gamma')
    _add_node(watcher, 'Epsilon')
    _add_node(watcher, 'Zeta')
    _add_node(watcher, 'Eta')

    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_removing_nodes_below_minimum_count(watcher: NetworkInconsistencyWatcher):
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.connect('Gamma')
    watcher.connect('Delta')
    _remove_node(watcher, 'Delta')

    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_removing_nodes_and_going_below_consensus(watcher: NetworkInconsistencyWatcher):
    _add_node(watcher, 'Theta')
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.connect('Gamma')
    watcher.connect('Theta')
    watcher.disconnect('Beta')
    watcher.disconnect('Gamma')
    _remove_node(watcher, 'Theta')

    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_just_three_nodes_connect_and_disconnect_in_7_node_pool(
        watcher: NetworkInconsistencyWatcher):
    watcher.set_nodes(['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'Zeta', 'Eta'])
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.connect('Gamma')
    watcher.disconnect('Alpha')
    watcher.disconnect('Beta')
    watcher.disconnect('Gamma')

    assert watcher.callback.call_count == 0
