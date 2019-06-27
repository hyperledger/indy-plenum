import pytest

from plenum.common.event_bus import ExternalBus
from plenum.test.greek import genNodeNames
from plenum.test.helper import MockTimer
from plenum.test.simulation.sim_network import SimNetwork
from plenum.test.simulation.sim_random import DefaultSimRandom
from plenum.test.test_event_bus import SomeMessage, create_some_message


NODE_COUNT = 5


class TestNode:
    def __init__(self, name: str, network: ExternalBus):
        self.name = name
        self.network = network
        self.received = []

        network.subscribe(SomeMessage, self.process_some_message)

    def process_some_message(self, message: SomeMessage, frm: str):
        self.received.append((message, frm))


@pytest.fixture
def mock_timer():
    return MockTimer()


@pytest.fixture
def test_nodes(mock_timer):
    random = DefaultSimRandom()
    net = SimNetwork(mock_timer, random)
    return [TestNode(name, net.create_peer(name)) for name in genNodeNames(NODE_COUNT)]


@pytest.fixture(params=range(NODE_COUNT))
def some_node(request, test_nodes):
    return test_nodes[request.param]


@pytest.fixture(params=range(NODE_COUNT-1))
def other_node(request, test_nodes, some_node):
    available_nodes = [node for node in test_nodes
                       if node != some_node]
    return available_nodes[request.param]


@pytest.fixture(params=range(NODE_COUNT-2))
def another_node(request, test_nodes, some_node, other_node):
    available_nodes = [node for node in test_nodes
                       if node not in [some_node, other_node]]
    return available_nodes[request.param]


def test_sim_network_broadcast(mock_timer, test_nodes, some_node):
    should_receive = [node for node in test_nodes if node != some_node]

    message = create_some_message()
    some_node.network.send(message)

    # Make sure messages are not delivered immediately
    for node in test_nodes:
        assert not node.received

    # Make sure messages are delivered eventually, but not to sending node
    mock_timer.run_to_completion()
    assert not some_node.received
    for node in should_receive:
        assert node.received == [(message, some_node.name)]


def test_sim_network_unicast(mock_timer, test_nodes, some_node, other_node):
    should_not_receive = [node for node in test_nodes if node != other_node]

    message = create_some_message()
    some_node.network.send(message, other_node.name)

    # Make sure messages are not delivered immediately
    for node in test_nodes:
        assert not node.received

    # Make sure message is delivered only to recipient
    mock_timer.run_to_completion()
    assert other_node.received == [(message, some_node.name)]
    for node in should_not_receive:
        assert not node.received


def test_sim_network_multicast(mock_timer, test_nodes, some_node, other_node, another_node):
    should_not_receive = [node for node in test_nodes
                          if node not in [other_node, another_node]]

    message = create_some_message()
    some_node.network.send(message, [other_node.name, another_node.name])

    # Make sure messages are not delivered immediately
    for node in test_nodes:
        assert not node.received

    # Make sure message is delivered only to recipient
    mock_timer.run_to_completion()
    assert other_node.received == [(message, some_node.name)]
    assert another_node.received == [(message, some_node.name)]
    for node in should_not_receive:
        assert not node.received
