import base58
import pytest
import re

import time

from plenum.common.constants import GET_TXN
from plenum.server.validator_info_tool import ValidatorNodeInfoTool
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from stp_core.common.constants import ZMQ_NETWORK_PROTOCOL

nodeCount = 5
MAX_TIME_FOR_INFO_BUILDING = 3


def test_validator_info_file_alias_field_valid(info):
    assert info['Node_info']['Name'] == 'Alpha'


def test_validator_info_file_bindings_field_valid(info, node):
    assert info['Node_info']['Client_ip'] == node.clientstack.ha.host
    assert info['Node_info']['Client_port'] == node.clientstack.ha.port
    assert info['Node_info']['Client_protocol'] == ZMQ_NETWORK_PROTOCOL
    assert info['Node_info']['Node_ip'] == node.nodestack.ha.host
    assert info['Node_info']['Node_port'] == node.nodestack.ha.port
    assert info['Node_info']['Node_protocol'] == ZMQ_NETWORK_PROTOCOL


def test_validator_info_file_did_field_valid(info):
    assert info['Node_info']['did'] == 'JpYerf4CssDrH76z7jyQPJLnZ1vwYgvKbvcp16AB5RQ'


def test_validator_info_file_bls_field_valid(info, node):
    assert info['Node_info']['BLS_key'] == node.bls_bft.bls_crypto_signer.pk


def test_validator_info_file_response_version_field_valid(info):
    assert info['response-version'] == ValidatorNodeInfoTool.JSON_SCHEMA_VERSION


def test_validator_info_file_timestamp_field_valid(node, info, looper):
    looper.runFor(2)
    assert re.match('\d{10}', str(info['timestamp']))
    latest_info = node._info_tool.info
    assert latest_info['timestamp'] > info['timestamp']


def test_validator_info_file_verkey_field_valid(node, info):
    assert info['Node_info']['verkey'] == base58.b58encode(node.nodestack.verKey).decode("utf-8")


def test_validator_info_file_metrics_avg_write_field_valid(info,
                                                           write_txn_and_get_latest_info):
    assert info['Node_info']['Metrics']['average-per-second']['write-transactions'] == 0
    latest_info = write_txn_and_get_latest_info()
    assert latest_info['Node_info']['Metrics']['average-per-second']['write-transactions'] > 0


def test_validator_info_file_metrics_avg_read_field_valid(info,
                                                          read_txn_and_get_latest_info
                                                          ):
    assert info['Node_info']['Metrics']['average-per-second']['read-transactions'] == 0
    latest_info = read_txn_and_get_latest_info(GET_TXN)
    assert latest_info['Node_info']['Metrics']['average-per-second']['read-transactions'] > 0


def test_validator_info_file_metrics_count_ledger_field_valid(node, info):
    txns_num = node.domainLedger.size
    assert info['Node_info']['Metrics']['transaction-count']['ledger'] == txns_num


def test_validator_info_file_metrics_count_config_field_valid(node, info):
    txns_num = node.configLedger.size
    assert info['Node_info']['Metrics']['transaction-count']['config'] == txns_num


def test_validator_info_file_metrics_count_pool_field_valid(info):
    assert info['Node_info']['Metrics']['transaction-count']['pool'] == nodeCount


def test_validator_info_file_metrics_uptime_field_valid(looper, node, info):
    assert info['Node_info']['Metrics']['uptime'] > 0
    looper.runFor(2)
    latest_info = node._info_tool.info
    assert latest_info['Node_info']['Metrics']['uptime'] > info['Node_info']['Metrics']['uptime']


def test_validator_info_file_pool_fields_valid(looper, info, txnPoolNodesLooper, txnPoolNodeSet, node):
    assert info['Pool_info']['Reachable_nodes_count'] == nodeCount
    assert info['Pool_info']['Reachable_nodes'] == [("Alpha", 0), ("Beta", 1), ("Delta", None), ("Epsilon", None), ("Gamma", None)]
    assert info['Pool_info']['Unreachable_nodes_count'] == 0
    assert info['Pool_info']['Unreachable_nodes'] == []
    assert info['Pool_info']['Total_nodes_count'] == nodeCount

    others, disconnected = txnPoolNodeSet[:-1], txnPoolNodeSet[-1]
    disconnect_node_and_ensure_disconnected(txnPoolNodesLooper, txnPoolNodeSet, disconnected)
    latest_info = node._info_tool.info

    assert latest_info['Pool_info']['Reachable_nodes_count'] == nodeCount - 1
    assert latest_info['Pool_info']['Reachable_nodes'] == [("Alpha", 0), ("Beta", 1), ("Delta", None), ("Gamma", None)]
    assert latest_info['Pool_info']['Unreachable_nodes_count'] == 1
    assert latest_info['Pool_info']['Unreachable_nodes'] == [("Epsilon", None)]
    assert latest_info['Pool_info']['Total_nodes_count'] == nodeCount


@pytest.mark.skip(reason="info will not be included by default")
def test_hardware_info_section(info):
    assert info['Hardware']
    assert info['Hardware']['HDD_all']
    assert info['Hardware']['RAM_all_free']
    assert info['Hardware']['RAM_used_by_node']
    assert info['Hardware']['HDD_used_by_node']


def test_software_info_section(info):
    assert info['Software']
    assert info['Software']['OS_version']
    assert info['Software']['Installed_packages']
    assert info['Software']['Indy_packages']


def test_node_info_section(info, node):
    assert info['Node_info']
    assert info['Node_info']['Catchup_status']
    assert info['Node_info']['Catchup_status']['Last_txn_3PC_keys']
    assert len(info['Node_info']['Catchup_status']['Last_txn_3PC_keys']) == 3
    assert info['Node_info']['Catchup_status']['Ledger_statuses']
    assert len(info['Node_info']['Catchup_status']['Ledger_statuses']) == 3
    assert info['Node_info']['Catchup_status']['Number_txns_in_catchup']
    # TODO uncomment this, when this field would be implemented
    # assert info['Node_info']['Catchup_status']['Received_LedgerStatus']
    assert info['Node_info']['Catchup_status']['Waiting_consistency_proof_msgs']
    assert len(info['Node_info']['Catchup_status']['Waiting_consistency_proof_msgs']) == 3
    assert info['Node_info']['Count_of_replicas']
    # TODO uncomment this, when this field would be implemented
    # assert info['Node_info']['Last N pool ledger txns']
    assert info['Node_info']['Metrics']
    assert info['Node_info']['Mode']
    assert info['Node_info']['Name']
    assert info['Node_info']['Replicas_status']
    assert "Committed_ledger_root_hashes" in info['Node_info']
    assert "Committed_state_root_hashes" in info['Node_info']
    assert "Uncommitted_ledger_root_hashes" in info['Node_info']
    assert "Uncommitted_ledger_txns" in info['Node_info']
    assert "Uncommitted_state_root_hashes" in info['Node_info']
    assert info['Node_info']['View_change_status']
    assert 'IC_queue'       in info['Node_info']['View_change_status']
    assert 'VCDone_queue'   in info['Node_info']['View_change_status']
    assert 'VC_in_progress' in info['Node_info']['View_change_status']
    assert 'View_No'        in info['Node_info']['View_change_status']
    assert 'Last_complete_view_no' in info['Node_info']['View_change_status']
    assert 'Last_view_change_started_at' in info['Node_info']['View_change_status']
    assert info['Node_info']['Requests_timeouts']
    assert 'Propagates_phase_req_timeouts' in info['Node_info']['Requests_timeouts']
    assert 'Ordering_phase_req_timeouts' in info['Node_info']['Requests_timeouts']


def test_pool_info_section(info):
    assert info['Pool_info']
    assert info['Pool_info']["Total_nodes_count"]
    assert 'Blacklisted_nodes' in info['Pool_info']
    assert info['Pool_info']['Quorums']
    assert info['Pool_info']['Reachable_nodes']
    assert 'Read_only' in info['Pool_info']
    assert 'Suspicious_nodes' in info['Pool_info']
    assert 'Unreachable_nodes' in info['Pool_info']
    assert 'Reachable_nodes' in info['Pool_info']
    assert "Reachable_nodes_count" in info['Pool_info']
    assert "Unreachable_nodes_count" in info['Pool_info']
    assert info['Pool_info']['f_value']


def test_config_info_section(node):
    info = node._info_tool.additional_info
    assert 'Main_config' in info['Configuration']['Config']
    assert 'Network_config' in info['Configuration']['Config']
    assert 'User_config' in info['Configuration']['Config']
    assert info['Configuration']['Config']['Main_config']
    assert info['Configuration']['Config']['Network_config']
    assert info['Configuration']['Genesis_txns']
    assert 'indy.env' in info['Configuration']
    assert 'node_control.conf' in info['Configuration']
    assert 'indy-node.service' in info['Configuration']
    assert 'indy-node-control.service' in info['Configuration']
    assert 'iptables_config' in info['Configuration']


@pytest.mark.skip(reason="info will not be included by default")
def test_extractions_section(node):
    info = node._info_tool.info
    assert "journalctl_exceptions" in info["Extractions"]
    assert "indy-node_status" in info["Extractions"]
    assert "node-control status" in info["Extractions"]
    assert "upgrade_log" in info["Extractions"]
    assert "stops_stat" in info["Extractions"]
    # TODO effective with rocksdb as storage type.
    # In leveldb it would be iteration over all txns

    # assert "Last_N_pool_ledger_txns" in info["Extractions"]
    # assert "Last_N_domain_ledger_txns" in info["Extractions"]
    # assert "Last_N_config_ledger_txns" in info["Extractions"]


# TODO effective with rocksdb as storage type.
# In leveldb it would be iteration over all txns

# def test_last_exactly_N_txn_from_ledger(node,
#                                         looper,
#                                         txnPoolNodeSet,
#                                         sdk_pool_handle,
#                                         sdk_wallet_steward):
#     txnCount = 10
#     sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, txnCount)
#     assert node.domainLedger.size > NUMBER_TXNS_FOR_DISPLAY
#     extractions = node._info_tool.additional_info['Extractions']
#     assert len(extractions["Last_N_domain_ledger_txns"]) == NUMBER_TXNS_FOR_DISPLAY


def test_build_node_info_time(node):
    after = time.perf_counter()
    node._info_tool.info
    before = time.perf_counter()
    assert before - after < MAX_TIME_FOR_INFO_BUILDING


def test_protocol_info_section(info):
    assert 'Protocol' in info


@pytest.fixture
def write_txn_and_get_latest_info(txnPoolNodesLooper,
                                  sdk_pool_handle,
                                  sdk_wallet_client,
                                  node):
    def write_wrapped():
        sdk_send_random_and_check(txnPoolNodesLooper, range(nodeCount),
                                  sdk_pool_handle,
                                  sdk_wallet_client,
                                  1)
        return node._info_tool.info

    return write_wrapped
