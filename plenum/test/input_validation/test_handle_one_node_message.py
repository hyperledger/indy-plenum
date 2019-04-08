from plenum.common.messages.node_messages import Batch, InstanceChange


def test_unpack_node_msg_with_str_as_msg_in_batch(create_node_and_not_start):
    node = create_node_and_not_start
    while node.nodeInBox:
        node.nodeInBox.pop()
    batch = Batch(['pi',
                   '{"op": "INSTANCE_CHANGE",'
                   ' "viewNo": 1, '
                   ' "reason": 25}'], None, frm='SomeNode', ts_rcv=1)
    node.unpackNodeMsg(batch)
    assert len(node.nodeInBox) == 1
    m = node.nodeInBox.pop()
    assert isinstance(m, InstanceChange)
