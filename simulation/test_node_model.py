from node_model import NodeModel, Quorum, InstanceChange


def test_instance_change_with_same_ids_trigger_view_change():
    model = NodeModel(1, Quorum(4))
    assert model.process_instance_change(2, InstanceChange(view_no=1, id=0)) == []
    assert model.process_instance_change(3, InstanceChange(view_no=1, id=0)) == []
    assert model.process_instance_change(4, InstanceChange(view_no=1, id=0)) != []


def test_instance_change_with_different_ids_dont_trigger_view_change():
    model = NodeModel(1, Quorum(4))
    assert model.process_instance_change(2, InstanceChange(view_no=1, id=0)) == []
    assert model.process_instance_change(3, InstanceChange(view_no=1, id=1)) == []
    assert model.process_instance_change(4, InstanceChange(view_no=1, id=0)) == []
