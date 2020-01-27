import pytest

from plenum.common.constants import POOL_LEDGER_ID, VALIDATOR, CURRENT_PROTOCOL_VERSION, NODE, DATA, TYPE, CLIENT_IP, \
    ALIAS, CLIENT_PORT, NODE_IP, NODE_PORT, BLS_KEY, SERVICES, TARGET_NYM, NYM, DOMAIN_LEDGER_ID
from plenum.common.request import Request
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.test.input_validation.constants import TEST_IDENTIFIER_SHORT


def test_load_regs_from_pool_ledger_on_initial_catchup_finished(node_reg_handler, write_req_manager):
    assert node_reg_handler.uncommitted_node_reg == []
    assert node_reg_handler.committed_node_reg == []
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 0
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert node_reg_handler.active_node_reg == []

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']


def test_update_node_regs_on_apply_txns(node_reg_handler, init_node_reg_handler, write_req_manager):
    three_pc_batch1 = add_node(write_req_manager, "Epsilon", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert three_pc_batch1.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    three_pc_batch2 = add_node(write_req_manager, "AAA", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert three_pc_batch1.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert three_pc_batch2.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']

    three_pc_batch3 = demote_node(write_req_manager, "Beta", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert three_pc_batch1.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert three_pc_batch2.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert three_pc_batch3.node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA']

    three_pc_batch4 = add_node(write_req_manager, "Beta", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert three_pc_batch1.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert three_pc_batch2.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert three_pc_batch3.node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert three_pc_batch4.node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'Beta']


def test_update_node_reg_at_beginning_of_view_updated_on_commit_only(node_reg_handler, init_node_reg_handler,
                                                                     write_req_manager):
    three_pc_batch1 = add_node(write_req_manager, "Epsilon", view_no=1, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2
    assert three_pc_batch1.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    three_pc_batch2 = add_node(write_req_manager, "AAA", view_no=2, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3
    assert three_pc_batch2.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']

    three_pc_batch3 = add_node(write_req_manager, "BBB", view_no=3, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 4
    assert three_pc_batch3.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'BBB']

    write_req_manager.commit_batch(three_pc_batch1)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 4
    assert three_pc_batch1.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    write_req_manager.commit_batch(three_pc_batch2)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[2] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3
    assert three_pc_batch2.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']

    write_req_manager.commit_batch(three_pc_batch3)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[2] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon',
                                                                           'AAA']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2
    assert three_pc_batch3.node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'BBB']


def test_update_node_regs_on_commit_txns(node_reg_handler, init_node_reg_handler, write_req_manager):
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    add_node(write_req_manager, "AAA", view_no=0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    demote_node(write_req_manager, "Beta", view_no=0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    add_node(write_req_manager, "Beta", view_no=0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'Beta']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1


def test_update_node_regs_on_apply_and_commit_txns(node_reg_handler, init_node_reg_handler,
                                                   write_req_manager):
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    three_pc_batch1 = add_node(write_req_manager, "AAA", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    three_pc_batch2 = demote_node(write_req_manager, "Gamma", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    write_req_manager.commit_batch(three_pc_batch1)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    write_req_manager.commit_batch(three_pc_batch2)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1


def test_update_node_regs_on_revert_txns(node_reg_handler, init_node_reg_handler,
                                         write_req_manager):
    add_node(write_req_manager, "CCC", view_no=0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    add_node(write_req_manager, "Epsilon", view_no=0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    add_node(write_req_manager, "AAA", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    demote_node(write_req_manager, "Beta", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Gamma', 'Delta', 'CCC', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'CCC', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1


def test_update_node_regs_on_apply_and_commit_in_different_views(node_reg_handler, init_node_reg_handler,
                                                                 write_req_manager):
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    demote_node(write_req_manager, "Alpha", view_no=0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1

    demote_node(write_req_manager, "Beta", view_no=1, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    add_node(write_req_manager, "BBB", view_no=1, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    three_pc_batch1 = add_node(write_req_manager, "AAA", view_no=2, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    three_pc_batch2 = add_node(write_req_manager, "Beta", view_no=2, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    three_pc_batch3 = add_node(write_req_manager, "Alpha", view_no=3, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 4

    three_pc_batch4 = add_node(write_req_manager, "CCC", view_no=3, commit=False, original_view_no=3)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 4

    write_req_manager.commit_batch(three_pc_batch1)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch2)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch3)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                           'Beta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    write_req_manager.commit_batch(three_pc_batch4)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                           'Beta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2


def test_update_node_regs_on_apply_and_commit_in_different_views_original_view_no(node_reg_handler,
                                                                                  init_node_reg_handler,
                                                                                  write_req_manager):
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True, original_view_no=0)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    demote_node(write_req_manager, "Alpha", view_no=0, commit=True, original_view_no=0)
    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    demote_node(write_req_manager, "Beta", view_no=1, commit=True, original_view_no=0)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    add_node(write_req_manager, "BBB", view_no=1, commit=True, original_view_no=0)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    three_pc_batch1 = add_node(write_req_manager, "AAA", view_no=2, commit=False, original_view_no=0)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 1

    three_pc_batch2 = add_node(write_req_manager, "Beta", view_no=2, commit=False, original_view_no=1)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    three_pc_batch3 = add_node(write_req_manager, "Alpha", view_no=3, commit=False, original_view_no=1)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    three_pc_batch4 = add_node(write_req_manager, "CCC", view_no=3, commit=False, original_view_no=3)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta',
                                                'Alpha']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta',
                                                                             'Alpha']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch1)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta',
                                                'Alpha']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta',
                                                                             'Alpha']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch2)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta',
                                                'Alpha']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta',
                                                                             'Alpha']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch3)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta',
                                                'Alpha']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta', 'Alpha']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch4)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha', 'CCC']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta',
                                                'Alpha']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                           'Beta', 'Alpha']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA',
                                                                             'Beta', 'Alpha']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2


def test_update_node_regs_on_revert_in_different_views(node_reg_handler, init_node_reg_handler,
                                                       write_req_manager):
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 1

    add_node(write_req_manager, "AAA", view_no=1, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    demote_node(write_req_manager, "Gamma", view_no=1, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    demote_node(write_req_manager, "Beta", view_no=3, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    add_node(write_req_manager, "BBB", view_no=3, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 1


def test_update_node_regs_on_apply_after_revert_in_different_views(node_reg_handler, init_node_reg_handler,
                                                                   write_req_manager):
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True)
    add_node(write_req_manager, "AAA", view_no=1, commit=False)
    demote_node(write_req_manager, "Gamma", view_no=1, commit=False)
    demote_node(write_req_manager, "Beta", view_no=3, commit=False)
    add_node(write_req_manager, "BBB", view_no=3, commit=False)

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)

    add_node(write_req_manager, "SSS", view_no=1, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'SSS']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    demote_node(write_req_manager, "Gamma", view_no=3, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'SSS']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'SSS']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                             'Epsilon', 'SSS']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3


def test_update_node_regs_on_revert_in_different_views_original_view_no(node_reg_handler, init_node_reg_handler,
                                                                        write_req_manager):
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True, original_view_no=0)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    add_node(write_req_manager, "AAA", view_no=1, commit=False, original_view_no=0)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    demote_node(write_req_manager, "Gamma", view_no=1, commit=False, original_view_no=0)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    demote_node(write_req_manager, "Beta", view_no=2, commit=False, original_view_no=0)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    add_node(write_req_manager, "BBB", view_no=2, commit=False, original_view_no=1)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    add_node(write_req_manager, "CCC", view_no=2, commit=False, original_view_no=2)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA', 'BBB', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[2] == ['Alpha', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[1] == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 1

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 1

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 1

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 1


def test_clean_node_regs_on_commit_txns(node_reg_handler, init_node_reg_handler, write_req_manager):
    three_pc_batch1 = add_node(write_req_manager, "AAA", view_no=0, commit=False)
    three_pc_batch2 = add_node(write_req_manager, "BBB", view_no=3, commit=False)
    three_pc_batch3 = add_node(write_req_manager, "CCC", view_no=6, commit=False)
    three_pc_batch4 = add_node(write_req_manager, "DDD", view_no=9, commit=False)
    three_pc_batch5 = add_node(write_req_manager, "EEE", view_no=10, commit=False)
    three_pc_batch6 = add_node(write_req_manager, "FFF", view_no=12, commit=False)

    write_req_manager.commit_batch(three_pc_batch1)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB', 'CCC', 'DDD',
                                                     'EEE', 'FFF']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB',
                                                'CCC', 'DDD', 'EEE']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[6] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                             'BBB']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[9] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                             'BBB', 'CCC']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[10] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[12] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD', 'EEE']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 6

    write_req_manager.commit_batch(three_pc_batch2)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB', 'CCC', 'DDD',
                                                     'EEE', 'FFF']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB',
                                                'CCC', 'DDD', 'EEE']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[6] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                             'BBB']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[9] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                             'BBB', 'CCC']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[10] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[12] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD', 'EEE']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 6

    write_req_manager.commit_batch(three_pc_batch3)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB', 'CCC', 'DDD',
                                                     'EEE', 'FFF']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB', 'CCC']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB',
                                                'CCC', 'DDD', 'EEE']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[6] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                           'BBB']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[3] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[6] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                             'BBB']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[9] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                             'BBB', 'CCC']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[10] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[12] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD', 'EEE']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 5

    write_req_manager.commit_batch(three_pc_batch4)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB', 'CCC', 'DDD',
                                                     'EEE', 'FFF']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB', 'CCC', 'DDD']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB',
                                                'CCC', 'DDD', 'EEE']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[6] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                           'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[9] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                           'BBB',
                                                                           'CCC']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[6] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                             'BBB']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[9] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                             'BBB', 'CCC']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[10] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[12] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD', 'EEE']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 4

    write_req_manager.commit_batch(three_pc_batch5)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB', 'CCC', 'DDD',
                                                     'EEE', 'FFF']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB', 'CCC', 'DDD', 'EEE']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB',
                                                'CCC', 'DDD', 'EEE']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[9] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                           'BBB',
                                                                           'CCC']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[10] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                            'BBB',
                                                                            'CCC', 'DDD']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[9] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                             'BBB', 'CCC']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[10] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[12] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD', 'EEE']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch6)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB', 'CCC', 'DDD',
                                                     'EEE', 'FFF']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB', 'CCC', 'DDD', 'EEE',
                                                   'FFF']
    assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA', 'BBB',
                                                'CCC', 'DDD', 'EEE']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[10] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                            'BBB',
                                                                            'CCC', 'DDD']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[12] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                            'BBB',
                                                                            'CCC', 'DDD', 'EEE']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[10] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[12] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'AAA',
                                                                              'BBB', 'CCC', 'DDD', 'EEE']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2


@pytest.mark.parametrize('add_node_reg_to_audit', [True, False])
@pytest.mark.parametrize('first_txn', [None, 'DomainFirst', 'PoolFirst'])
def test_load_regs_on_catchup_finished_view_initial_to_0(node_reg_handler, init_node_reg_handler,
                                                         write_req_manager,
                                                         add_node_reg_to_audit,
                                                         first_txn):
    if first_txn == 'PoolFirst':
        edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    elif first_txn == 'DomainFirst':
        add_domain_txn(write_req_manager, view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    demote_node(write_req_manager, "Alpha", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    if first_txn is None:
        # as we don't know whether the first Audit txn was for a Pool or other ledger Batch,
        # get all Nodes for a Pool ledger corresponding to the first audit txn
        assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
        assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                               'Epsilon']
    else:
        assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
        assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
        assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 1
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view


@pytest.mark.parametrize('add_node_reg_to_audit', [True, False])
@pytest.mark.parametrize('first_txn', [None, 'DomainFirst', 'PoolFirst'])
@pytest.mark.parametrize('view_no', [1, 2])
def test_load_regs_on_catchup_finished_view_initial_to_X(node_reg_handler, init_node_reg_handler,
                                                         write_req_manager,
                                                         add_node_reg_to_audit,
                                                         first_txn,
                                                         view_no):
    if first_txn == 'PoolFirst':
        edit_node(write_req_manager, "Gamma", view_no=view_no, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    elif first_txn == 'DomainFirst':
        add_domain_txn(write_req_manager, view_no=view_no, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    add_node(write_req_manager, "Epsilon", view_no=view_no, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    edit_node(write_req_manager, "Gamma", view_no=view_no, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    demote_node(write_req_manager, "Alpha", view_no=view_no, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    if first_txn is None:
        # as we don't know whether the first Audit txn was for a Pool or other ledger Batch,
        # get all Nodes for a Pool ledger corresponding to the first audit txn
        assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
        assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                                     'Epsilon']
        assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta',
                                                                               'Epsilon']
    else:
        assert node_reg_handler.active_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
        assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Alpha', 'Beta', 'Gamma', 'Delta']
        assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
        assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view


@pytest.mark.parametrize('add_node_reg_to_audit', ['True', 'False', 'Latest_only'])
@pytest.mark.parametrize('view_no', [1, 2])
def test_load_regs_on_catchup_finished_views_from_0_to_X_list_as_first_node_reg_in_view(node_reg_handler,
                                                                                        init_node_reg_handler,
                                                                                        write_req_manager,
                                                                                        add_node_reg_to_audit,
                                                                                        view_no):
    add_node_reg_to_audit_view_0 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_1 = add_node_reg_to_audit == 'True' or add_node_reg_to_audit == 'Latest_only'

    add_node(write_req_manager, "Epsilon", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    demote_node(write_req_manager, "Alpha", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)

    demote_node(write_req_manager, "Beta", view_no=view_no, commit=True,
                add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    edit_node(write_req_manager, "Gamma", view_no=view_no, commit=True,
              add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    add_node(write_req_manager, "BBB", view_no=view_no, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view


@pytest.mark.parametrize('add_node_reg_to_audit', ['True', 'False', 'Latest_only'])
@pytest.mark.parametrize('view_no', [1, 2])
def test_load_regs_on_catchup_finished_views_from_0_to_X_delta_as_first_node_reg_in_view(node_reg_handler,
                                                                                         init_node_reg_handler,
                                                                                         write_req_manager,
                                                                                         add_node_reg_to_audit,
                                                                                         view_no):
    add_node_reg_to_audit_view_0 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_1 = add_node_reg_to_audit == 'True' or add_node_reg_to_audit == 'Latest_only'

    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    demote_node(write_req_manager, "Alpha", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)

    edit_node(write_req_manager, "Gamma", view_no=view_no, commit=True,
              add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    demote_node(write_req_manager, "Beta", view_no=view_no, commit=True,
                add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    edit_node(write_req_manager, "Gamma", view_no=view_no, commit=True,
              add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    add_node(write_req_manager, "BBB", view_no=view_no, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view


@pytest.mark.parametrize('add_node_reg_to_audit', ['True', 'False', 'Latest_only'])
def test_load_regs_on_catchup_finished_views_0_1_2_list_as_first_node_reg_in_view(node_reg_handler,
                                                                                  init_node_reg_handler,
                                                                                  write_req_manager,
                                                                                  add_node_reg_to_audit):
    add_node_reg_to_audit_view_0 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_1 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_2 = add_node_reg_to_audit == 'True' or add_node_reg_to_audit == 'Latest_only'

    add_node(write_req_manager, "Epsilon", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    demote_node(write_req_manager, "Alpha", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)

    demote_node(write_req_manager, "Beta", view_no=1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    edit_node(write_req_manager, "Gamma", view_no=1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    add_node(write_req_manager, "BBB", view_no=1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)

    demote_node(write_req_manager, "Epsilon", view_no=2, commit=True,
                add_node_reg_to_audit=add_node_reg_to_audit_view_2)
    edit_node(write_req_manager, "Gamma", view_no=2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)
    add_node(write_req_manager, "AAA", view_no=2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'BBB', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'BBB', 'AAA']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view


@pytest.mark.parametrize('add_node_reg_to_audit', ['True', 'False', 'Latest_only'])
def test_load_regs_on_catchup_finished_views_0_1_2_delta_as_first_node_reg_in_view(node_reg_handler,
                                                                                   init_node_reg_handler,
                                                                                   write_req_manager,
                                                                                   add_node_reg_to_audit):
    add_node_reg_to_audit_view_0 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_1 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_2 = add_node_reg_to_audit == 'True' or add_node_reg_to_audit == 'Latest_only'

    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    demote_node(write_req_manager, "Alpha", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)

    edit_node(write_req_manager, "Gamma", view_no=1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    demote_node(write_req_manager, "Beta", view_no=1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    edit_node(write_req_manager, "Gamma", view_no=1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    add_node(write_req_manager, "BBB", view_no=1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)

    edit_node(write_req_manager, "Gamma", view_no=2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)
    demote_node(write_req_manager, "Epsilon", view_no=2, commit=True,
                add_node_reg_to_audit=add_node_reg_to_audit_view_2)
    edit_node(write_req_manager, "Gamma", view_no=2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)
    add_node(write_req_manager, "AAA", view_no=2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'BBB', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'BBB', 'AAA']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view


def test_load_regs_on_catchup_finished_with_uncomitted(node_reg_handler, init_node_reg_handler,
                                                       write_req_manager):
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True)
    demote_node(write_req_manager, "Alpha", view_no=0, commit=True)
    demote_node(write_req_manager, "Beta", view_no=1, commit=True)
    add_node(write_req_manager, "BBB", view_no=1, commit=True)
    add_node(write_req_manager, "AAA", view_no=2, commit=True)
    add_node(write_req_manager, "Beta", view_no=2, commit=True)
    add_node(write_req_manager, "Alpha", view_no=3, commit=False)
    add_node(write_req_manager, "CCC", view_no=3, commit=False)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view


def test_update_node_regs_on_node_txn_formats_on_apply(node_reg_handler, init_node_reg_handler, write_req_manager):
    add_node(write_req_manager, "Epsilon", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    demote_node(write_req_manager, "Unknown1", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    edit_node(write_req_manager, "Unknown2", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    edit_node(write_req_manager, "Alpha", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    add_domain_txn(write_req_manager, view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    edit_node(write_req_manager, "AAA", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    demote_node(write_req_manager, "Alpha", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']

    demote_node(write_req_manager, "Beta", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon']

    add_node(write_req_manager, "Beta", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta']

    add_node(write_req_manager, "AAA", view_no=0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta', 'AAA']


@pytest.mark.parametrize('add_node_reg_to_audit', [True, False])
def test_update_node_regs_on_node_txn_formats_on_catchup(node_reg_handler, init_node_reg_handler, write_req_manager,
                                                         add_node_reg_to_audit):
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    demote_node(write_req_manager, "Unknown", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    edit_node(write_req_manager, "Unknown", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    edit_node(write_req_manager, "Alpha", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    edit_node(write_req_manager, "AAA", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    write_req_manager.on_catchup_finished(),
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    demote_node(write_req_manager, "Alpha", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']

    demote_node(write_req_manager, "Beta", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon']

    add_node(write_req_manager, "Beta", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta']

    add_node(write_req_manager, "AAA", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta', 'AAA']


@pytest.mark.parametrize('add_node_reg_to_audit', ['True', 'False', 'Latest_only'])
@pytest.mark.parametrize('view_no', [1, 2])
def test_apply_uncommitted_after_cathup_finished_on_same_view(node_reg_handler,
                                                              init_node_reg_handler,
                                                              write_req_manager,
                                                              add_node_reg_to_audit,
                                                              view_no):
    add_node_reg_to_audit_view_0 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_1 = add_node_reg_to_audit == 'True' or add_node_reg_to_audit == 'Latest_only'

    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    demote_node(write_req_manager, "Alpha", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)

    edit_node(write_req_manager, "Gamma", view_no=view_no, commit=True,
              add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    demote_node(write_req_manager, "Beta", view_no=view_no, commit=True,
                add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    edit_node(write_req_manager, "Gamma", view_no=view_no, commit=True,
              add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    add_node(write_req_manager, "BBB", view_no=view_no, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    three_pc_batch1 = add_node(write_req_manager, "CCC", view_no=view_no, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    three_pc_batch2 = add_node(write_req_manager, "DDD", view_no=view_no + 1, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC', 'DDD']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no + 1] == ['Gamma', 'Delta', 'Epsilon',
                                                                                       'BBB', 'CCC']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch1)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC', 'DDD']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no + 1] == ['Gamma', 'Delta', 'Epsilon',
                                                                                       'BBB', 'CCC']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch2)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC', 'DDD']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC', 'DDD']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no + 1] == ['Gamma', 'Delta', 'Epsilon', 'BBB',
                                                                                     'CCC']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no + 1] == ['Gamma', 'Delta', 'Epsilon',
                                                                                       'BBB', 'CCC']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2


@pytest.mark.parametrize('add_node_reg_to_audit', ['True', 'False', 'Latest_only'])
@pytest.mark.parametrize('view_no', [1, 2])
def test_apply_uncommitted_after_cathup_finished_on_next_view(node_reg_handler,
                                                              init_node_reg_handler,
                                                              write_req_manager,
                                                              add_node_reg_to_audit,
                                                              view_no):
    add_node_reg_to_audit_view_0 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_1 = add_node_reg_to_audit == 'True' or add_node_reg_to_audit == 'Latest_only'

    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    add_node(write_req_manager, "Epsilon", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    edit_node(write_req_manager, "Gamma", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    demote_node(write_req_manager, "Alpha", view_no=0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)

    edit_node(write_req_manager, "Gamma", view_no=view_no, commit=True,
              add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    demote_node(write_req_manager, "Beta", view_no=view_no, commit=True,
                add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    edit_node(write_req_manager, "Gamma", view_no=view_no, commit=True,
              add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    add_node(write_req_manager, "BBB", view_no=view_no, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.committed_node_reg_at_beginning_of_view == node_reg_handler.uncommitted_node_reg_at_beginning_of_view

    three_pc_batch1 = add_node(write_req_manager, "CCC", view_no=view_no + 1, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no + 1] == ['Gamma', 'Delta', 'Epsilon',
                                                                                       'BBB']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    three_pc_batch2 = add_node(write_req_manager, "DDD", view_no=view_no + 1, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC', 'DDD']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no + 1] == ['Gamma', 'Delta', 'Epsilon',
                                                                                       'BBB']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch1)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC', 'DDD']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no + 1] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no + 1] == ['Gamma', 'Delta', 'Epsilon',
                                                                                       'BBB']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2

    write_req_manager.commit_batch(three_pc_batch2)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC', 'DDD']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'CCC', 'DDD']
    assert node_reg_handler.active_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg_at_beginning_of_view[view_no + 1] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert len(node_reg_handler.committed_node_reg_at_beginning_of_view) == 2
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.uncommitted_node_reg_at_beginning_of_view[view_no + 1] == ['Gamma', 'Delta', 'Epsilon',
                                                                                       'BBB']
    assert len(node_reg_handler.uncommitted_node_reg_at_beginning_of_view) == 2


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
        },
        TARGET_NYM: node_name
    }
    if services is not None:
        operation[DATA][SERVICES] = services

    return Request(operation=operation, reqId=1513945121191691,
                   protocolVersion=CURRENT_PROTOCOL_VERSION, identifier="6ouriXMZkLeHsuXrN1X1fd")


def build_domain_txn():
    operation = {
        TYPE: NYM,
        TARGET_NYM: TEST_IDENTIFIER_SHORT
    }
    return Request(operation=operation, reqId=1513945121191691,
                   protocolVersion=CURRENT_PROTOCOL_VERSION, identifier="6ouriXMZkLeHsuXrN1X1fd")


pp_seq_no = 0


def apply_req(write_req_manager, req, view_no, commit=True, add_node_reg_to_audit=True,
              original_view_no=None, ledger_id=POOL_LEDGER_ID):
    write_req_manager.apply_request(req, 1234)
    global pp_seq_no
    pp_seq_no += 1
    three_pc_batch = ThreePcBatch(ledger_id=ledger_id,
                                  inst_id=0,
                                  view_no=view_no,
                                  pp_seq_no=pp_seq_no,
                                  pp_time=1234,
                                  state_root=write_req_manager.database_manager.get_state_root_hash(ledger_id),
                                  txn_root=write_req_manager.database_manager.get_txn_root_hash(ledger_id),
                                  # make sure that we write new primaries every time
                                  # so that just one jump is not sufficient to find out the first txn in the last view
                                  primaries=["Node{}".format(pp_seq_no)],
                                  valid_digests=['digest1'],
                                  pp_digest='pp_digest',
                                  original_view_no=original_view_no)

    if not add_node_reg_to_audit:
        three_pc_batch.node_reg = None
        write_req_manager.audit_b_handler.post_batch_applied(three_pc_batch)
        write_req_manager.node_reg_handler.post_batch_applied(three_pc_batch)
    else:
        write_req_manager.post_apply_batch(three_pc_batch)

    if commit:
        write_req_manager.commit_batch(three_pc_batch)
    return three_pc_batch


def add_node(write_req_manager,
             node_name, view_no,
             commit=True,
             add_node_reg_to_audit=True,
             original_view_no=None):
    req = build_node_req(node_name, [VALIDATOR])
    return apply_req(write_req_manager, req, view_no, commit, add_node_reg_to_audit, original_view_no)


def demote_node(write_req_manager,
                node_name, view_no,
                commit=True,
                add_node_reg_to_audit=True,
                original_view_no=None):
    req = build_node_req(node_name, [])
    return apply_req(write_req_manager, req, view_no, commit, add_node_reg_to_audit, original_view_no)


def edit_node(write_req_manager,
              node_name, view_no,
              commit=True,
              add_node_reg_to_audit=True,
              original_view_no=None):
    req = build_node_req(node_name, None)
    return apply_req(write_req_manager, req, view_no, commit, add_node_reg_to_audit, original_view_no)


def add_domain_txn(write_req_manager,
                   view_no,
                   commit=True,
                   add_node_reg_to_audit=True,
                   original_view_no=None):
    req = build_domain_txn()
    return apply_req(write_req_manager, req, view_no, commit, add_node_reg_to_audit, original_view_no,
                     ledger_id=DOMAIN_LEDGER_ID)
