import pytest

from plenum.common.constants import POOL_LEDGER_ID, VALIDATOR, CURRENT_PROTOCOL_VERSION, NODE, DATA, TYPE, CLIENT_IP, \
    ALIAS, CLIENT_PORT, NODE_IP, NODE_PORT, BLS_KEY, SERVICES, TARGET_NYM
from plenum.common.request import Request
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch


def test_load_regs_from_pool_ledger_on_initial_catchup_finished(node_reg_handler, write_req_manager):
    assert node_reg_handler.uncommitted_node_reg == []
    assert node_reg_handler.committed_node_reg == []
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 0

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1


def test_update_node_regs_on_apply_txns(node_reg_handler, init_node_reg_handler, write_req_manager):
    three_pc_batch = add_node(write_req_manager, "Epsilon", 1, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1
    assert three_pc_batch.node_reg == node_reg_handler.uncommitted_node_reg

    three_pc_batch = add_node(write_req_manager, "AAA", 2, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1
    assert three_pc_batch.node_reg == node_reg_handler.uncommitted_node_reg

    three_pc_batch = demote_node(write_req_manager, "Beta", 3, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1
    assert three_pc_batch.node_reg == node_reg_handler.uncommitted_node_reg

    three_pc_batch = add_node(write_req_manager, "Beta", 4, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1
    assert three_pc_batch.node_reg == node_reg_handler.uncommitted_node_reg


def test_update_node_regs_on_commit_txns(node_reg_handler, init_node_reg_handler, write_req_manager):
    add_node(write_req_manager, "Epsilon", 1, 0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    add_node(write_req_manager, "AAA", 2, 0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    demote_node(write_req_manager, "Beta", 3, 0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    add_node(write_req_manager, "Beta", 4, 0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA', 'Beta']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1


def test_update_node_regs_on_apply_and_commit_txns(node_reg_handler, init_node_reg_handler,
                                                   write_req_manager):
    add_node(write_req_manager, "Epsilon", 1, 0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    three_pc_batch1 = add_node(write_req_manager, "AAA", 2, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    three_pc_batch2 = demote_node(write_req_manager, "Gamma", 3, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    write_req_manager.commit_batch(three_pc_batch1)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    write_req_manager.commit_batch(three_pc_batch2)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1


def test_update_node_regs_on_revert_txns(node_reg_handler, init_node_reg_handler,
                                         write_req_manager):
    add_node(write_req_manager, "Epsilon", 1, 0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    add_node(write_req_manager, "AAA", 2, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    demote_node(write_req_manager, "Beta", 3, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1


def test_update_node_regs_on_apply_and_commit_in_different_views(node_reg_handler, init_node_reg_handler,
                                                                 write_req_manager):
    add_node(write_req_manager, "Epsilon", 1, 0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    demote_node(write_req_manager, "Alpha", 2, 0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    demote_node(write_req_manager, "Beta", 3, 1, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2

    add_node(write_req_manager, "BBB", 4, 1, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2

    three_pc_batch1 = add_node(write_req_manager, "AAA", 5, 2, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 3

    three_pc_batch2 = add_node(write_req_manager, "Beta", 6, 2, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 3

    three_pc_batch3 = add_node(write_req_manager, "Alpha", 7, 3, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta',
                                                                 'Alpha']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 4

    write_req_manager.commit_batch(three_pc_batch1)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta',
                                                                 'Alpha']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch2)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta',
                                                                 'Alpha']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 3

    write_req_manager.commit_batch(three_pc_batch3)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta', 'Alpha']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[3] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta',
                                                                 'Alpha']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2


def test_update_node_regs_on_revert_in_different_views(node_reg_handler, init_node_reg_handler,
                                                       write_req_manager):
    add_node(write_req_manager, "Epsilon", 1, 0, commit=True)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1

    add_node(write_req_manager, "AAA", 2, 1, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2

    demote_node(write_req_manager, "Gamma", 3, 1, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2

    demote_node(write_req_manager, "Beta", 4, 2, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 3

    add_node(write_req_manager, "BBB", 5, 2, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 3

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Alpha', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 3

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'AAA']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2

    write_req_manager.post_batch_rejected(POOL_LEDGER_ID)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1


@pytest.mark.parametrize('add_node_reg_to_audit', [True, False])
@pytest.mark.parametrize('node_reg_as_delta', [True, False])
def test_load_regs_on_catchup_finished_view_0(node_reg_handler, init_node_reg_handler,
                                              write_req_manager,
                                              add_node_reg_to_audit,
                                              node_reg_as_delta):
    if node_reg_as_delta:
        edit_node(write_req_manager, "Gamma", 1, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    add_node(write_req_manager, "Epsilon", 2, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    edit_node(write_req_manager, "Gamma", 3, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    demote_node(write_req_manager, "Alpha", 4, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1


@pytest.mark.parametrize('add_node_reg_to_audit', [True, False])
@pytest.mark.parametrize('node_reg_as_delta', [True, False])
def test_load_regs_on_catchup_finished_view_1(node_reg_handler, init_node_reg_handler,
                                              write_req_manager,
                                              add_node_reg_to_audit,
                                              node_reg_as_delta):
    if node_reg_as_delta:
        edit_node(write_req_manager, "Gamma", 1, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    add_node(write_req_manager, "Epsilon", 2, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    edit_node(write_req_manager, "Gamma", 3, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)
    demote_node(write_req_manager, "Alpha", 4, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 1


@pytest.mark.parametrize('add_node_reg_to_audit', ['True', 'False', 'Latest_only'])
def test_load_regs_on_catchup_finished_views_0_1_list_as_first_node_reg_in_view(node_reg_handler, init_node_reg_handler,
                                                                                write_req_manager,
                                                                                add_node_reg_to_audit):
    add_node_reg_to_audit_view_0 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_1 = add_node_reg_to_audit == 'True' or add_node_reg_to_audit == 'Latest_only'

    add_node(write_req_manager, "Epsilon", 2, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    edit_node(write_req_manager, "Gamma", 3, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    demote_node(write_req_manager, "Alpha", 4, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)

    demote_node(write_req_manager, "Beta", 6, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    edit_node(write_req_manager, "Gamma", 7, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    add_node(write_req_manager, "BBB", 8, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2


@pytest.mark.parametrize('add_node_reg_to_audit', ['True', 'False', 'Latest_only'])
def test_load_regs_on_catchup_finished_views_0_1_delta_as_first_node_reg_in_view(node_reg_handler,
                                                                                 init_node_reg_handler,
                                                                                 write_req_manager,
                                                                                 add_node_reg_to_audit):
    add_node_reg_to_audit_view_0 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_1 = add_node_reg_to_audit == 'True' or add_node_reg_to_audit == 'Latest_only'

    edit_node(write_req_manager, "Gamma", 1, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    add_node(write_req_manager, "Epsilon", 2, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    edit_node(write_req_manager, "Gamma", 3, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    demote_node(write_req_manager, "Alpha", 4, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)

    edit_node(write_req_manager, "Gamma", 5, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    demote_node(write_req_manager, "Beta", 6, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    edit_node(write_req_manager, "Gamma", 7, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    add_node(write_req_manager, "BBB", 8, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert node_reg_handler.node_reg_at_beginning_of_view[0] == ['Alpha', 'Beta', 'Gamma', 'Delta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2


@pytest.mark.parametrize('add_node_reg_to_audit', ['True', 'False', 'Latest_only'])
@pytest.mark.parametrize('node_reg_as_delta', [True, False])
def test_load_regs_on_catchup_finished_views_0_1_2_list_as_first_node_reg_in_view(node_reg_handler,
                                                                                  init_node_reg_handler,
                                                                                  write_req_manager,
                                                                                  add_node_reg_to_audit,
                                                                                  node_reg_as_delta):
    add_node_reg_to_audit_view_0 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_1 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_2 = add_node_reg_to_audit == 'True' or add_node_reg_to_audit == 'Latest_only'

    add_node(write_req_manager, "Epsilon", 1, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    edit_node(write_req_manager, "Gamma", 2, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    demote_node(write_req_manager, "Alpha", 3, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)

    demote_node(write_req_manager, "Beta", 4, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    edit_node(write_req_manager, "Gamma", 5, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    add_node(write_req_manager, "BBB", 6, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)

    demote_node(write_req_manager, "Epsilon", 7, 2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)
    edit_node(write_req_manager, "Gamma", 8, 2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)
    add_node(write_req_manager, "AAA", 9, 2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'BBB', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'BBB', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'BBB']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2


@pytest.mark.parametrize('add_node_reg_to_audit', ['True', 'False', 'Latest_only'])
def test_load_regs_on_catchup_finished_views_0_1_2_delta_as_first_node_reg_in_view(node_reg_handler,
                                                                                   init_node_reg_handler,
                                                                                   write_req_manager,
                                                                                   add_node_reg_to_audit):
    add_node_reg_to_audit_view_0 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_1 = add_node_reg_to_audit == 'True'
    add_node_reg_to_audit_view_2 = add_node_reg_to_audit == 'True' or add_node_reg_to_audit == 'Latest_only'

    edit_node(write_req_manager, "Gamma", 1, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    add_node(write_req_manager, "Epsilon", 2, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    edit_node(write_req_manager, "Gamma", 3, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)
    demote_node(write_req_manager, "Alpha", 4, 0, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_0)

    edit_node(write_req_manager, "Gamma", 5, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    demote_node(write_req_manager, "Beta", 6, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    edit_node(write_req_manager, "Gamma", 7, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)
    add_node(write_req_manager, "BBB", 8, 1, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_1)

    edit_node(write_req_manager, "Gamma", 9, 2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)
    demote_node(write_req_manager, "Epsilon", 10, 2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)
    edit_node(write_req_manager, "Gamma", 11, 2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)
    add_node(write_req_manager, "AAA", 12, 2, commit=True, add_node_reg_to_audit=add_node_reg_to_audit_view_2)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'BBB', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'BBB', 'AAA']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2


def test_load_regs_on_catchup_finished_with_uncomitted(node_reg_handler, init_node_reg_handler,
                                                       write_req_manager):
    add_node(write_req_manager, "Epsilon", 1, 0, commit=True)
    demote_node(write_req_manager, "Alpha", 2, 0, commit=True)
    demote_node(write_req_manager, "Beta", 3, 1, commit=True)
    add_node(write_req_manager, "BBB", 4, 1, commit=True)
    add_node(write_req_manager, "AAA", 5, 2, commit=True)
    add_node(write_req_manager, "Beta", 6, 2, commit=True)
    add_node(write_req_manager, "Alpha", 7, 3, commit=False)
    add_node(write_req_manager, "CCC", 8, 3, commit=False)

    write_req_manager.on_catchup_finished()

    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA', 'Beta']
    assert node_reg_handler.node_reg_at_beginning_of_view[1] == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.node_reg_at_beginning_of_view[2] == ['Gamma', 'Delta', 'Epsilon', 'BBB', 'AAA']
    assert len(node_reg_handler.node_reg_at_beginning_of_view) == 2


def test_update_node_regs_on_node_txn_formats_on_apply(node_reg_handler, init_node_reg_handler, write_req_manager):
    add_node(write_req_manager, "Epsilon", 1, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    edit_node(write_req_manager, "Alpha", 2, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    edit_node(write_req_manager, "AAA", 3, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    demote_node(write_req_manager, "Alpha", 4, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']

    demote_node(write_req_manager, "Beta", 5, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon']

    add_node(write_req_manager, "Beta", 6, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta']

    add_node(write_req_manager, "AAA", 7, 0, commit=False)
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta', 'AAA']


def test_update_node_regs_on_node_txn_formats_on_catchup(node_reg_handler, init_node_reg_handler, write_req_manager):
    add_node(write_req_manager, "Epsilon", 1, 0, commit=True)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    edit_node(write_req_manager, "Alpha", 2, 0, commit=True)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    edit_node(write_req_manager, "AAA", 3, 0, commit=True)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon']

    demote_node(write_req_manager, "Alpha", 4, 0, commit=True)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Beta', 'Gamma', 'Delta', 'Epsilon']

    demote_node(write_req_manager, "Beta", 5, 0, commit=True)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon']

    add_node(write_req_manager, "Beta", 6, 0, commit=True)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta']

    add_node(write_req_manager, "AAA", 7, 0, commit=True)
    write_req_manager.on_catchup_finished()
    assert node_reg_handler.uncommitted_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta', 'AAA']
    assert node_reg_handler.committed_node_reg == ['Gamma', 'Delta', 'Epsilon', 'Beta', 'AAA']


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


def apply_req(write_req_manager, req, pp_seq_no, view_no, commit=True, add_node_reg_to_audit=True):
    write_req_manager.apply_request(req, 1234)
    three_pc_batch = ThreePcBatch(ledger_id=POOL_LEDGER_ID,
                                  inst_id=0,
                                  view_no=view_no,
                                  pp_seq_no=pp_seq_no,
                                  pp_time=1234,
                                  state_root=write_req_manager.database_manager.get_state_root_hash(POOL_LEDGER_ID),
                                  txn_root=write_req_manager.database_manager.get_txn_root_hash(POOL_LEDGER_ID),
                                  primaries=["Node{}".format(view_no)],
                                  valid_digests=['digest1'],
                                  pp_digest='pp_digest')

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
             node_name, pp_seq_no, view_no,
             commit=True,
             add_node_reg_to_audit=True):
    req = build_node_req(node_name, [VALIDATOR])
    return apply_req(write_req_manager, req, pp_seq_no, view_no, commit, add_node_reg_to_audit)


def demote_node(write_req_manager,
                node_name, pp_seq_no, view_no,
                commit=True,
                add_node_reg_to_audit=True):
    req = build_node_req(node_name, [])
    return apply_req(write_req_manager, req, pp_seq_no, view_no, commit, add_node_reg_to_audit)


def edit_node(write_req_manager,
              node_name, pp_seq_no, view_no,
              commit=True,
              add_node_reg_to_audit=True):
    req = build_node_req(node_name, None)
    return apply_req(write_req_manager, req, pp_seq_no, view_no, commit, add_node_reg_to_audit)
