def test_send_with_clientstack_restarts(looper,
                                        txnPoolNodeSet):
    for _ in range(10):
        for node in txnPoolNodeSet:
            node.restart_clientstack()
