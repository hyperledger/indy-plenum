from plenum.server.quota_control import QuotaControl

MAX_NODE_MESSAGE_COUNT_LIMIT = 100
MAX_NODE_MESSAGE_SIZE_LIMIT = 1024 * 1024
MAX_CLIENT_MESSAGE_COUNT_LIMIT = 20
MAX_CLIENT_MESSAGE_SIZE_LIMIT = 24 * 1024


def create_quota_control() -> QuotaControl:
    return QuotaControl(max_node_message_count_limit=MAX_NODE_MESSAGE_COUNT_LIMIT,
                        max_node_message_size_limit=MAX_NODE_MESSAGE_SIZE_LIMIT,
                        max_client_message_count_limit=MAX_CLIENT_MESSAGE_COUNT_LIMIT,
                        max_client_message_size_limit=MAX_CLIENT_MESSAGE_SIZE_LIMIT)


def test_quota_control_gives_maximum_quotas_initially():
    qc = create_quota_control()
    assert qc.node_message_count_limit == MAX_NODE_MESSAGE_COUNT_LIMIT
    assert qc.node_message_size_limit == MAX_NODE_MESSAGE_SIZE_LIMIT
    assert qc.client_message_count_limit == MAX_CLIENT_MESSAGE_COUNT_LIMIT
    assert qc.client_message_size_limit == MAX_CLIENT_MESSAGE_SIZE_LIMIT


def test_quota_control_gives_maximum_quotas_when_none_are_reached():
    qc = create_quota_control()
    qc.received_node_messages(MAX_NODE_MESSAGE_COUNT_LIMIT // 2, MAX_NODE_MESSAGE_SIZE_LIMIT // 2)
    qc.received_client_messages(MAX_CLIENT_MESSAGE_COUNT_LIMIT // 2, MAX_CLIENT_MESSAGE_SIZE_LIMIT // 2)
    assert qc.node_message_count_limit == MAX_NODE_MESSAGE_COUNT_LIMIT
    assert qc.node_message_size_limit == MAX_NODE_MESSAGE_SIZE_LIMIT
    assert qc.client_message_count_limit == MAX_CLIENT_MESSAGE_COUNT_LIMIT
    assert qc.client_message_size_limit == MAX_CLIENT_MESSAGE_SIZE_LIMIT


def test_quote_control_gives_no_quota_for_client_when_node_quota_is_reached():
    qc = create_quota_control()
    qc.received_node_messages(MAX_NODE_MESSAGE_COUNT_LIMIT, MAX_NODE_MESSAGE_SIZE_LIMIT)
    assert qc.node_message_count_limit == MAX_NODE_MESSAGE_COUNT_LIMIT
    assert qc.node_message_size_limit == MAX_NODE_MESSAGE_SIZE_LIMIT
    assert qc.client_message_count_limit == 0
    assert qc.client_message_size_limit == 0


def test_quote_control_restores_client_quotas_when_node_quota_is_no_longer_reached():
    qc = create_quota_control()
    qc.received_node_messages(MAX_NODE_MESSAGE_COUNT_LIMIT, MAX_NODE_MESSAGE_SIZE_LIMIT)
    qc.received_node_messages(MAX_NODE_MESSAGE_COUNT_LIMIT // 2, MAX_NODE_MESSAGE_SIZE_LIMIT // 2)
    assert qc.node_message_count_limit == MAX_NODE_MESSAGE_COUNT_LIMIT
    assert qc.node_message_size_limit == MAX_NODE_MESSAGE_SIZE_LIMIT
    assert qc.client_message_count_limit == MAX_CLIENT_MESSAGE_COUNT_LIMIT
    assert qc.client_message_size_limit == MAX_CLIENT_MESSAGE_SIZE_LIMIT
