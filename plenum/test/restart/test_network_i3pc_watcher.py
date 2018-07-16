from plenum.server.i3pc_watchers import NetworkI3PCWatcher

DEFAULT_NODE_SET = {'Alpha', 'Beta', 'Gamma', 'Delta'}


class WatcherCallbackMock:
    def __init__(self):
        self.call_count = 0

    def __call__(self):
        self.call_count += 1


def test_watcher_is_not_triggered_when_created():
    cb = WatcherCallbackMock()
    NetworkI3PCWatcher(DEFAULT_NODE_SET, cb)
    assert cb.call_count == 0


def test_watcher_is_not_triggered_when_nodes_are_initially_connected():
    cb = WatcherCallbackMock()
    watcher = NetworkI3PCWatcher(DEFAULT_NODE_SET, cb)

    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.connect('Gamma')
    watcher.connect('Delta')

    assert cb.call_count == 0


def test_watcher_is_not_triggered_when_just_one_node_connects_and_disconnects():
    cb = WatcherCallbackMock()
    watcher = NetworkI3PCWatcher(DEFAULT_NODE_SET, cb)

    watcher.connect('Alpha')
    watcher.disconnect('Alpha')

    assert cb.call_count == 0


def test_watcher_is_triggered_when_going_below_consensus():
    cb = WatcherCallbackMock()
    watcher = NetworkI3PCWatcher(DEFAULT_NODE_SET, cb)

    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.disconnect('Beta')

    assert cb.call_count == 1


def test_watcher_is_not_triggered_when_adding_nodes_while_on_edge_of_consensus():
    cb = WatcherCallbackMock()
    watcher = NetworkI3PCWatcher(DEFAULT_NODE_SET, cb)

    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.add_node('Epsilon')
    watcher.add_node('Zeta')
    watcher.add_node('Eta')

    assert cb.call_count == 0


def test_watcher_is_not_triggered_when_removing_nodes_below_minimum_count():
    cb = WatcherCallbackMock()
    watcher = NetworkI3PCWatcher(DEFAULT_NODE_SET, cb)

    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.connect('Gamma')
    watcher.connect('Delta')
    watcher.remove_node('Delta')

    assert cb.call_count == 0


def test_watcher_is_not_triggered_when_removing_nodes_and_going_below_consensus():
    cb = WatcherCallbackMock()
    watcher = NetworkI3PCWatcher(DEFAULT_NODE_SET, cb)

    watcher.add_node('Theta')
    watcher.connect('Alpha')
    watcher.connect('Theta')
    watcher.remove_node('Theta')

    assert cb.call_count == 0


def test_watcher_is_not_triggered_when_just_two_nodes_connect_and_disconnect_in_7_node_pool():
    cb = WatcherCallbackMock()
    watcher = NetworkI3PCWatcher(DEFAULT_NODE_SET, cb)

    watcher.add_node('Epsilon')
    watcher.add_node('Zeta')
    watcher.add_node('Eta')
    watcher.connect('Alpha')
    watcher.connect('Beta')
    watcher.disconnect('Alpha')
    watcher.disconnect('Beta')

    assert cb.call_count == 0
