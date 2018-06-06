import time


def test_record_node_msgs(some_txns_done, txnPoolNodeSet):
    recorders = []
    for node in txnPoolNodeSet:
        assert node.nodestack.recorder.store.size > 0
        assert node.clientstack.recorder.store.size > 0
        node.nodestack.recorder.start_playing()
        node.clientstack.recorder.start_playing()
        recorders.append(node.nodestack.recorder)
        recorders.append(node.clientstack.recorder)

    # Drain each recorder
    while any([recorder.is_playing for recorder in recorders]):
        for recorder in recorders:
            recorder.get_next()
        time.sleep(0.01)
