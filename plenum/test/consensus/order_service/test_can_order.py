from plenum.common.startable import Mode


def test_can_order(orderer):
    assert orderer.can_order_commits()


def test_cant_order_not_participating(orderer, mode_not_participating):
    orderer._data.node_mode = mode_not_participating
    assert not orderer.can_order_commits()


def test_can_order_synced_and_view_change(orderer):
    orderer._data.node_mode = Mode.synced
    orderer._data.legacy_vc_in_progress = True
    assert orderer.can_order_commits()
