from plenum.server.quorums import Quorums
from plenum.simulation.node_model import NodeModel, InstanceChange
from plenum.simulation.pool_connections import PoolConnections
from plenum.simulation.timer_model import TimerModel


def test_instance_change_with_same_ids_trigger_view_change():
    timer = TimerModel()
    connections = PoolConnections()
    model = NodeModel(1, Quorums(4), timer, connections)
    assert model.process_instance_change(2, InstanceChange(view_no=1, id=0)) == []
    assert model.process_instance_change(3, InstanceChange(view_no=1, id=0)) == []
    assert model.process_instance_change(4, InstanceChange(view_no=1, id=0)) != []


def test_instance_change_with_different_ids_dont_trigger_view_change():
    timer = TimerModel()
    connections = PoolConnections()
    model = NodeModel(1, Quorums(4), timer, connections)
    assert model.process_instance_change(2, InstanceChange(view_no=1, id=0)) == []
    assert model.process_instance_change(3, InstanceChange(view_no=1, id=1)) == []
    assert model.process_instance_change(4, InstanceChange(view_no=1, id=0)) == []
