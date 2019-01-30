from node_model import NodeModel, Quorum, InstanceChange
from pool_connections import PoolConnections


def test_instance_change_with_same_ids_trigger_view_change():
    connections = PoolConnections()
    model = NodeModel(1, Quorum(4), connections)
    assert model.process_instance_change(0, 2, InstanceChange(view_no=1, id=0)) == []
    assert model.process_instance_change(0, 3, InstanceChange(view_no=1, id=0)) == []
    assert model.process_instance_change(0, 4, InstanceChange(view_no=1, id=0)) != []


def test_instance_change_with_different_ids_dont_trigger_view_change():
    connections = PoolConnections()
    model = NodeModel(1, Quorum(4), connections)
    assert model.process_instance_change(0, 2, InstanceChange(view_no=1, id=0)) == []
    assert model.process_instance_change(0, 3, InstanceChange(view_no=1, id=1)) == []
    assert model.process_instance_change(0, 4, InstanceChange(view_no=1, id=0)) == []
