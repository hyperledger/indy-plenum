from typing import Tuple

import pytest

from plenum.common.event_bus import ExternalBus
from plenum.common.timer import TimerService
from plenum.common.util import randomString
from plenum.test.greek import genNodeNames
from plenum.test.simulation.sim_network import SimNetwork
from plenum.test.simulation.sim_random import DefaultSimRandom
from plenum.test.test_event_bus import SomeMessage, create_some_message


NODE_COUNT = 5


class TestNode:
    def __init__(self, name: str, timer: TimerService, network: ExternalBus):
        self.name = name
        self.timer = timer
        self.network = network
        self.receive_timestamps = []
        self.received = []

        network.subscribe(SomeMessage, self.process_some_message)

    def process_some_message(self, message: SomeMessage, frm: str):
        self.receive_timestamps.append(self.timer.get_current_time())
        self.received.append((message, frm))


@pytest.fixture
def random():
    return DefaultSimRandom()


@pytest.fixture(params=[(1, 500), (0, 5), (10, 10)])
def latency_bounds(request) -> Tuple[int, int]:
    return request.param


@pytest.fixture
def sim_network(mock_timer, random, latency_bounds):
    net = SimNetwork(mock_timer, random)
    net.set_latency(latency_bounds[0], latency_bounds[1])
    return net


@pytest.fixture
def test_nodes(sim_network, mock_timer, random):
    names = [name for name in genNodeNames(NODE_COUNT)]
    names = random.shuffle(names)
    return [TestNode(name, mock_timer, sim_network.create_peer(name)) for name in names]


@pytest.fixture
def some_node(test_nodes, some_item):
    return some_item(test_nodes)


@pytest.fixture
def other_node(test_nodes, some_node, other_item):
    return other_item(test_nodes, exclude=[some_node])


@pytest.fixture
def another_node(test_nodes, some_node, other_node, another_item):
    return another_item(test_nodes, exclude=[some_node, other_node])


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


def test_sim_network_raises_on_sending_to_itself(some_node):
    message = create_some_message()
    with pytest.raises(AssertionError):
        some_node.network.send(message, some_node.name)


@pytest.mark.skip(reason="For now we need to allow sending to unknown nodes, but correct behavior is still TBD")
def test_sim_network_raises_on_sending_to_unknown(some_node):
    message = create_some_message()
    with pytest.raises(AssertionError):
        some_node.network.send(message, randomString(16))


def test_sim_network_raises_on_sending_to_no_one(some_node):
    message = create_some_message()
    with pytest.raises(AssertionError):
        some_node.network.send(message, [])


def test_sim_network_respects_latencies(random, test_nodes, mock_timer, initial_time, latency_bounds):
    for i in range(100):
        node = random.choice(*test_nodes)
        node.network.send(create_some_message())

    mock_timer.run_to_completion()

    min_ts = initial_time + latency_bounds[0]
    max_ts = initial_time + latency_bounds[1]

    for node in test_nodes:
        assert all(min_ts <= ts <= max_ts for ts in node.receive_timestamps)


def test_sim_network_broadcast_in_lexicographic_order(mock_timer, sim_network, test_nodes, some_node):
    latency = 10
    sim_network.set_latency(latency, latency)
    should_receive = sorted([node for node in test_nodes if node != some_node], key=lambda n: n.name)

    message = create_some_message()
    some_node.network.send(message)

    for i in range(len(should_receive) + 1):
        already_received = should_receive[:i]
        still_waiting = should_receive[i:]

        assert all(len(node.received) == 1 for node in already_received)
        assert all(not node.received for node in still_waiting)
        mock_timer.advance()
