from plenum.common.constants import POOL_LEDGER_ID, VALIDATOR, CURRENT_PROTOCOL_VERSION, NODE, DATA, TYPE, CLIENT_IP, \
    ALIAS, CLIENT_PORT, NODE_IP, NODE_PORT, BLS_KEY, SERVICES, TARGET_NYM
from plenum.common.request import Request
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch


def build_node_req(node_name, services):
    operation = {
        TYPE: NODE,
        DATA: {
            ALIAS: node_name,
            CLIENT_IP: '127.0.0.1',
            CLIENT_PORT: 7588,
            NODE_IP: '127.0.0.1',
            NODE_PORT: 7587,
            BLS_KEY: '00000000000000000000000000000000',
            SERVICES: services
        },
        TARGET_NYM: node_name
    }

    return Request(operation=operation, reqId=1513945121191691,
                   protocolVersion=CURRENT_PROTOCOL_VERSION, identifier="6ouriXMZkLeHsuXrN1X1fd")


def apply_req(write_req_manager, req, pp_seq_no, view_no, commit=True):
    write_req_manager.apply_request(req, 1234)
    three_pc_batch = ThreePcBatch(ledger_id=POOL_LEDGER_ID,
                                  inst_id=0,
                                  view_no=view_no,
                                  pp_seq_no=pp_seq_no,
                                  pp_time=1234,
                                  state_root=write_req_manager.database_manager.get_state_root_hash(POOL_LEDGER_ID),
                                  txn_root=write_req_manager.database_manager.get_txn_root_hash(POOL_LEDGER_ID),
                                  primaries=[],
                                  valid_digests=[],
                                  pp_digest='')
    write_req_manager.post_apply_batch(three_pc_batch)
    if commit:
        write_req_manager.commit_batch(three_pc_batch)


def add_node(write_req_manager,
             node_name, pp_seq_no, view_no,
             commit=True):
    req = build_node_req(node_name, [VALIDATOR])
    apply_req(write_req_manager, req, pp_seq_no, view_no, commit)


def demote_node(write_req_manager,
                node_name, pp_seq_no, view_no,
                commit=True):
    req = build_node_req(node_name, [])
    apply_req(write_req_manager, req, pp_seq_no, view_no, commit)


def test_load_regs_rom_pool_ledger_on_initial_catchup_finished(node_reg_handler, write_req_manager):
    assert node_reg_handler.uncommitted_node_reg == []
    assert node_reg_handler.committed_node_reg == []
    assert node_reg_handler.last_view_node_reg == []

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.last_view_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']

def test_update_uncommitted_node_reg_on(node_reg_handler, init_node_reg_handler, write_req_manager):
    add_node(write_req_manager, "Epsilon", 1, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.last_view_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']



# def test_update_node_regs_on_catchup(node_reg_handler, write_req_manager):
#     write_req_manager.on_catchup_finished()
#
#     add_node(write_req_manager, "Epsilon", 1, 1)
#     add_node(write_req_manager, "Node2", 1, 1)
#     add_node(write_req_manager, "Node1", 1, 1)
#     demote_node(write_req_manager, "Node1", 1, 1)
#     assert node_reg_handler.uncommitted_node_reg == initial_nodes
#     assert node_reg_handler.committed_node_reg == initial_nodes
#     assert node_reg_handler.last_view_node_reg == initial_nodes