from unittest.mock import Mock

from plenum.test.helper import MockTimer
from plenum.test.simulation.sim_network import SimNetwork
from plenum.test.simulation.sim_random import DefaultSimRandom
from plenum.test.test_event_bus import SomeMessage, create_some_message


def test_sim_network_can_create_nodes_which_communicate():
    message = create_some_message()

    timer = MockTimer()
    random = DefaultSimRandom()
    net = SimNetwork(timer, random)
    some_node = net.create_peer('some_node')
    other_node = net.create_peer('other_node')

    some_handler = Mock()
    other_node.subscribe(SomeMessage, some_handler)
    some_node.send(message)

    # Make sure events are not delivered immediately
    some_handler.assert_not_called()

    # Make sure events are delivered eventually
    timer.wait_for(lambda: timer.queue_size() == 0)
    some_handler.assert_called_once_with(message, 'some_node')
