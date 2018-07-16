import pytest

from plenum.server.i3pc_watchers import NetworkI3PCWatcher

DEFAULT_NODE_SET = {'Alpha', 'Beta', 'Gamma', 'Delta'}


class WatcherCallbackMock:
    def __init__(self):
        self.call_count = 0

    def __call__(self):
        self.call_count += 1


@pytest.fixture
def watcher():
    cb = WatcherCallbackMock()
    watcher = NetworkI3PCWatcher(cb)
    watcher.set_nodes(DEFAULT_NODE_SET)
    return watcher


def _add_node(watcher: NetworkI3PCWatcher, node: str):
    nodes = watcher.nodes
    nodes.add(node)
    watcher.set_nodes(nodes)


def _remove_node(watcher: NetworkI3PCWatcher, node: str):
    nodes = watcher.nodes
    nodes.discard(node)
    watcher.set_nodes(nodes)


def test_watcher_is_not_triggered_when_created(watcher: NetworkI3PCWatcher):
    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_nodes_are_initially_connected(watcher: NetworkI3PCWatcher):
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.connect('Gamma')
    watcher.connect('Delta')

    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_just_one_node_connects_and_disconnects(watcher: NetworkI3PCWatcher):
    watcher.connect('Alpha')
    watcher.disconnect('Alpha')

    assert watcher.callback.call_count == 0


def test_watcher_is_triggered_when_going_below_consensus(watcher: NetworkI3PCWatcher):
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.disconnect('Beta')

    assert watcher.callback.call_count == 1


def test_watcher_is_not_triggered_when_adding_nodes_while_on_edge_of_consensus(watcher: NetworkI3PCWatcher):
    watcher.connect('Alpha')
    watcher.connect('Beta')
    _add_node(watcher, 'Epsilon')
    _add_node(watcher, 'Zeta')
    _add_node(watcher, 'Eta')

    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_removing_nodes_below_minimum_count(watcher: NetworkI3PCWatcher):
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.connect('Gamma')
    watcher.connect('Delta')
    _remove_node(watcher, 'Delta')

    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_removing_nodes_and_going_below_consensus(watcher: NetworkI3PCWatcher):
    _add_node(watcher, 'Theta')
    watcher.connect('Alpha')
    watcher.connect('Theta')
    _remove_node(watcher, 'Theta')

    assert watcher.callback.call_count == 0


def test_watcher_is_not_triggered_when_just_two_nodes_connect_and_disconnect_in_7_node_pool(
        watcher: NetworkI3PCWatcher):
    watcher.set_nodes(['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'Zeta', 'Eta'])
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.disconnect('Alpha')
    watcher.disconnect('Beta')

    assert watcher.callback.call_count == 0
