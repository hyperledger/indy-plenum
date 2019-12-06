from plenum.server.consensus.utils import replica_name_to_node_name


def test_replica_name_to_node_name():
    assert replica_name_to_node_name('Alpha:0') == 'Alpha'
    assert replica_name_to_node_name('Beta:0') == 'Beta'
    assert replica_name_to_node_name('Alpha:10') == 'Alpha'
    assert replica_name_to_node_name('Alpha') == 'Alpha'
    assert replica_name_to_node_name('Evil:Formed:Name:-12') == 'Evil:Formed:Name'
    assert replica_name_to_node_name(None) is None
